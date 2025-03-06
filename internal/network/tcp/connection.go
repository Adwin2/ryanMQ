/*
连接管理

封包/拆包 （处理粘包）
心跳维护 （保活机制）

基于 net.Listener 实现非阻塞 IO
使用 goroutine-per-connection 模型（Go 高并发优势）
*/
package tcp

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"ryanMQ/internal/utils/rlog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	// 默认读写缓冲区大小
	DefaultReadBufferSize  = 4 * 1024 // 4KB
	DefaultWriteBufferSize = 4 * 1024 // 4KB

	// 心跳相关配置
	DefaultHeartbeatInterval = 5 * time.Second  // 心跳发送间隔
	DefaultIdleTimeout       = 30 * time.Second // 空闲超时时间

	// 接收消息缓冲队列大小
	DefaultMessageQueueSize = 100
)

var (
	ErrConnectionClosed = errors.New("connection closed")
	ErrReadTimeout      = errors.New("read timeout")
	ErrWriteTimeout     = errors.New("write timeout")
)

// TCPConnection 表示一个TCP连接
type TCPConnection struct {
	ID              string         // 连接唯一标识
	conn            net.Conn       // 底层TCP连接
	reader          *bufio.Reader  // 带缓冲的读取器
	writer          *bufio.Writer  // 带缓冲的写入器
	writeLock       sync.Mutex     // 写入锁，保证并发写入安全
	isClosed        int32          // 连接是否已关闭的原子标志
	lastActivity    int64          // 最后活动时间戳
	heartbeatTicker *time.Ticker   // 心跳定时器
	messageQueue    chan []byte    // 接收消息队列
	closeCallback   func(string)   // 连接关闭回调
	onMessage       func([]byte)   // 消息处理回调
	wg              sync.WaitGroup // 等待组，确保所有goroutine结束
}

// NewTCPConnection 创建一个新的TCP连接实例
func NewTCPConnection(conn net.Conn, onMessage func([]byte), closeCallback func(string)) *TCPConnection {
	c := &TCPConnection{
		ID:            uuid.New().String(),
		conn:          conn,
		reader:        bufio.NewReaderSize(conn, DefaultReadBufferSize),
		writer:        bufio.NewWriterSize(conn, DefaultWriteBufferSize),
		lastActivity:  time.Now().Unix(),
		messageQueue:  make(chan []byte, DefaultMessageQueueSize),
		onMessage:     onMessage,
		closeCallback: closeCallback,
	}

	return c
}

// Start 启动连接处理
func (c *TCPConnection) Start() {
	c.wg.Add(2)

	// 启动读取循环
	go c.readLoop()

	// 启动处理循环
	go c.processLoop()

	// 启动心跳检测
	c.startHeartbeat(context.Background())
}

// readLoop 持续从连接中读取消息
func (c *TCPConnection) readLoop() {
	defer c.wg.Done()
	defer c.Close()

	for {
		if atomic.LoadInt32(&c.isClosed) == 1 {
			return
		}

		// 设置读取超时
		c.conn.SetReadDeadline(time.Now().Add(DefaultIdleTimeout))

		// 读取消息长度
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(c.reader, lenBuf)
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				// 记录非正常关闭的错误
				rlog.Error("read message length error:", err)
			}
			return
		}

		// 更新最后活动时间
		atomic.StoreInt64(&c.lastActivity, time.Now().Unix())

		// 解析消息长度
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// 防止恶意的大消息攻击
		if msgLen > 10*1024*1024 { // 最大10MB
			rlog.Warn("read message length exceed limit")
			continue
		}

		// 读取消息内容
		msgBuf := make([]byte, msgLen)
		_, err = io.ReadFull(c.reader, msgBuf)
		if err != nil {
			rlog.Error("read message error:", err)
			return
		}

		// 将消息放入队列
		select {
		case c.messageQueue <- msgBuf:
			// 消息成功放入队列
		default:
			// 队列已满，可能需要处理背压
			rlog.Warn("message queue is full, dropping message")
		}
	}
}

// processLoop 处理接收到的消息
func (c *TCPConnection) processLoop() {
	defer c.wg.Done()

	for {
		select {
		case msg, ok := <-c.messageQueue:
			if !ok {
				return // 通道已关闭
			}

			// 调用消息处理回调
			if c.onMessage != nil {
				c.onMessage(msg)
			}

		case <-time.After(DefaultHeartbeatInterval):
			// 定期检查连接是否长时间无活动
			lastActive := atomic.LoadInt64(&c.lastActivity)
			if time.Now().Unix()-lastActive > int64(DefaultIdleTimeout.Seconds()) {
				// 连接超时，关闭它
				c.Close()
				return
			}
		}
	}
}

// Write 向连接写入数据
func (c *TCPConnection) Write(data []byte) error {
	if atomic.LoadInt32(&c.isClosed) == 1 {
		return ErrConnectionClosed
	}

	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	// 设置写入超时
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// 写入消息长度
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := c.writer.Write(lenBuf); err != nil {
		return err
	}

	// 写入消息内容
	if _, err := c.writer.Write(data); err != nil {
		return err
	}

	// 刷新缓冲区
	if err := c.writer.Flush(); err != nil {
		return err
	}

	// 更新最后活动时间
	atomic.StoreInt64(&c.lastActivity, time.Now().Unix())

	return nil
}

// startHeartbeat 开始心跳检测
func (c *TCPConnection) startHeartbeat(ctx context.Context) {
	c.heartbeatTicker = time.NewTicker(DefaultHeartbeatInterval)

	go func() {
		defer c.heartbeatTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.heartbeatTicker.C:
				if atomic.LoadInt32(&c.isClosed) == 1 {
					return
				}

				// 发送心跳包特殊格式
				heartbeatMsg := []byte("PING")
				if err := c.Write(heartbeatMsg); err != nil {
					// 心跳发送失败，可能需要关闭连接
					c.Close()
					return
				}
			}
		}
	}()
}

// Close 关闭连接
func (c *TCPConnection) Close() {
	// 使用CAS操作确保只关闭一次
	if !atomic.CompareAndSwapInt32(&c.isClosed, 0, 1) {
		return
	}

	// 关闭底层连接
	c.conn.Close()

	// 关闭消息队列
	close(c.messageQueue)

	// 通知连接已关闭
	if c.closeCallback != nil {
		c.closeCallback(c.ID)
	}

	// 等待所有goroutine结束
	c.wg.Wait()
}

// GetRemoteAddr 获取远程地址
func (c *TCPConnection) GetRemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// IsAlive 检查连接是否活跃
func (c *TCPConnection) IsAlive() bool {
	return atomic.LoadInt32(&c.isClosed) == 0
}

// SetMessageCallback 设置消息处理回调
func (c *TCPConnection) SetMessageCallback(callback func([]byte)) {
	c.onMessage = callback
}

// SetCloseCallback 设置连接关闭回调
func (c *TCPConnection) SetCloseCallback(callback func(string)) {
	c.closeCallback = callback
}
