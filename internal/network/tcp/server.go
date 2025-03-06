/*
TCP服务器

监听端口，接受连接
为每个连接启动处理协程
*/
package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// MessageHandler 定义了处理接收到消息的回调函数类型
type MessageHandler func(connID string, message []byte)

// Server 表示一个TCP服务器
type Server struct {
	address          string                    // 监听地址，格式为 "host:port"
	listener         net.Listener              // TCP监听器
	connectionMap    map[string]*TCPConnection // 连接映射表，key是连接ID
	connectionMapMux sync.RWMutex              // 连接映射表的读写锁
	messageHandler   MessageHandler            // 消息处理回调
	maxConnections   int                       // 最大连接数限制
	wg               sync.WaitGroup            // 等待组，确保所有goroutine结束
	ctx              context.Context           // 上下文，用于控制服务器生命周期
	cancel           context.CancelFunc        // 取消函数，用于停止服务器
}

// NewServer 创建一个新的TCP服务器实例
func NewServer(address string, maxConnections int) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		address:        address,
		connectionMap:  make(map[string]*TCPConnection),
		maxConnections: maxConnections,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start 启动TCP服务器
func (s *Server) Start() error {
	var err error

	// 创建TCP监听器
	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to start TCP server: %w", err)
	}

	fmt.Printf("TCP server started on %s\n", s.address)

	// 启动接受连接的循环
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// acceptLoop 持续接受新的连接
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		// 检查服务器是否已关闭
		select {
		case <-s.ctx.Done():
			return
		default:
			// 继续处理
		}

		// 设置接受连接的超时
		s.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))

		// 接受新连接
		conn, err := s.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 这只是一个超时，继续尝试
				continue
			}
			// 检查是否由于服务器关闭而停止接受
			select {
			case <-s.ctx.Done():
				return
			default:
				// 其他错误
				fmt.Printf("Error accepting connection: %v\n", err)
				continue
			}
		}

		// 检查是否超过最大连接数
		if s.maxConnections > 0 && s.GetConnectionCount() >= s.maxConnections {
			fmt.Println("Max connections reached, rejecting new connection")
			conn.Close()
			continue
		}

		// 处理新连接
		s.handleNewConnection(conn)
	}
}

// handleNewConnection 处理新接受的连接
func (s *Server) handleNewConnection(conn net.Conn) {
	// 创建TCP连接对象，暂时使用空的回调函数
	tcpConn := NewTCPConnection(
		conn,
		nil, // 暂时传递nil，稍后设置
		nil, // 暂时传递nil，稍后设置
	)
	
	// 设置消息处理回调
	tcpConn.SetMessageCallback(func(data []byte) {
		// 消息处理回调
		if s.messageHandler != nil {
			s.messageHandler(tcpConn.ID, data)
		}
	})
	
	// 设置连接关闭回调
	tcpConn.SetCloseCallback(func(connID string) {
		// 连接关闭回调
		s.removeConnection(connID)
	})
	
	// 将连接添加到映射表
	s.addConnection(tcpConn)

	// 启动连接处理
	tcpConn.Start()

	fmt.Printf("New connection accepted: %s (ID: %s)\n", tcpConn.GetRemoteAddr(), tcpConn.ID)
}

// Stop 停止TCP服务器
func (s *Server) Stop() {
	// 已经关闭，直接返回
	if s.ctx.Err() != nil {
		return
	}

	// 发送取消信号
	s.cancel()

	// 关闭监听器
	if s.listener != nil {
		s.listener.Close()
	}

	// 关闭所有连接
	s.closeAllConnections()

	// 等待所有goroutine结束
	s.wg.Wait()

	fmt.Println("TCP server stopped")
}

// SetMessageHandler 设置消息处理回调
func (s *Server) SetMessageHandler(handler MessageHandler) {
	s.messageHandler = handler
}

// Broadcast 向所有连接广播消息
func (s *Server) Broadcast(message []byte) {
	s.connectionMapMux.RLock()
	defer s.connectionMapMux.RUnlock()

	for _, conn := range s.connectionMap {
		// 异步发送，避免阻塞
		go func(c *TCPConnection, msg []byte) {
			if err := c.Write(msg); err != nil {
				fmt.Printf("Error broadcasting to connection %s: %v\n", c.ID, err)
			}
		}(conn, message)
	}
}

// SendToConnection 向指定连接发送消息
func (s *Server) SendToConnection(connID string, message []byte) error {
	s.connectionMapMux.RLock()
	conn, exists := s.connectionMap[connID]
	s.connectionMapMux.RUnlock()

	if !exists {
		return errors.New("connection not found")
	}

	return conn.Write(message)
}

// CloseConnection 关闭指定连接
func (s *Server) CloseConnection(connID string) {
	s.connectionMapMux.RLock()
	conn, exists := s.connectionMap[connID]
	s.connectionMapMux.RUnlock()

	if exists {
		conn.Close()
	}
}

// GetConnectionCount 获取当前连接数
func (s *Server) GetConnectionCount() int {
	s.connectionMapMux.RLock()
	defer s.connectionMapMux.RUnlock()

	return len(s.connectionMap)
}

// addConnection 将连接添加到映射表
func (s *Server) addConnection(conn *TCPConnection) {
	s.connectionMapMux.Lock()
	defer s.connectionMapMux.Unlock()

	s.connectionMap[conn.ID] = conn
}

// removeConnection 从映射表中移除连接
func (s *Server) removeConnection(connID string) {
	s.connectionMapMux.Lock()
	defer s.connectionMapMux.Unlock()

	delete(s.connectionMap, connID)
	fmt.Printf("Connection removed: %s\n", connID)
}

// closeAllConnections 关闭所有连接
func (s *Server) closeAllConnections() {
	s.connectionMapMux.Lock()
	conns := make([]*TCPConnection, 0, len(s.connectionMap))
	for _, conn := range s.connectionMap {
		conns = append(conns, conn)
	}
	s.connectionMapMux.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}
