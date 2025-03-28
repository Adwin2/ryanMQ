package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/raymond/ryanMQ/tutorial/v2/mq/core/storage"
)

var (
	ErrTopicClosed = errors.New("topic is closed")
)

// Topic 表示一个消息主题
type Topic struct {
	name            string
	log             *storage.Log
	mu              sync.RWMutex
	consumers       map[string][]chan *Message // 消费者组名 -> 消费者通道列表
	retentionPeriod time.Duration              // 消息保留时间
	closed          bool
	ctx             context.Context
	cancel          context.CancelFunc
}

// TopicConfig 主题配置
type TopicConfig struct {
	Name            string
	StorageDir      string
	SegmentMaxSize  uint64
	IndexInterval   uint64
	RetentionPeriod time.Duration
}

// NewTopic 创建一个新的主题
func NewTopic(config TopicConfig) (*Topic, error) {
	if config.Name == "" {
		return nil, errors.New("topic name cannot be empty")
	}

	// 如果未指定存储目录，使用默认目录
	if config.StorageDir == "" {
		config.StorageDir = fmt.Sprintf("data/topics/%s", config.Name)
	}

	// 如果未指定段大小，使用默认值
	if config.SegmentMaxSize == 0 {
		config.SegmentMaxSize = 1024 * 1024 * 1024 // 默认1GB
	}

	// 如果未指定索引间隔，使用默认值
	if config.IndexInterval == 0 {
		config.IndexInterval = 100 // 默认每100条消息建立索引
	}

	// 如果未指定保留时间，使用默认值
	if config.RetentionPeriod == 0 {
		config.RetentionPeriod = 7 * 24 * time.Hour // 默认7天
	}

	// 创建存储日志
	log, err := storage.NewLog(storage.LogConfig{
		Dir:            config.StorageDir,
		SegmentMaxSize: config.SegmentMaxSize,
		IndexInterval:  config.IndexInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create storage log: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	topic := &Topic{
		name:            config.Name,
		log:             log,
		consumers:       make(map[string][]chan *Message),
		retentionPeriod: config.RetentionPeriod,
		ctx:             ctx,
		cancel:          cancel,
	}

	// 启动后台任务
	go topic.cleanupTask()
	go topic.replayMessagesTask()

	return topic, nil
}

// Publish 发布消息到主题
func (t *Topic) Publish(data []byte) (*Message, error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, ErrTopicClosed
	}
	t.mu.RUnlock()

	// 创建消息
	msg := NewMessage(t.name, data)

	// 序列化消息
	msgData, err := msg.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	// 存储消息并获取偏移量
	offset, err := t.log.Append(msgData)
	if err != nil {
		return nil, fmt.Errorf("failed to append message to log: %w", err)
	}

	// 设置消息偏移量
	msg.Offset = offset

	// 分发给消费者
	t.distributeMessage(msg)

	return msg, nil
}

// Read 从指定偏移量读取消息
func (t *Topic) Read(offset uint64) (*Message, error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, ErrTopicClosed
	}
	t.mu.RUnlock()

	// 从日志读取消息数据
	data, err := t.log.Read(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message from log: %w", err)
	}

	// 反序列化消息
	msg := &Message{}
	if err := msg.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return msg, nil
}

// AddConsumer 添加消费者到主题
func (t *Topic) AddConsumer(groupName string, ch chan *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	// 获取组的消费者列表，如果不存在则创建
	chans, exists := t.consumers[groupName]
	if !exists {
		chans = make([]chan *Message, 0)
	}

	// 添加到消费者列表
	t.consumers[groupName] = append(chans, ch)
}

// RemoveConsumer 从主题移除消费者
func (t *Topic) RemoveConsumer(groupName string, ch chan *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	// 获取组的消费者列表
	chans, exists := t.consumers[groupName]
	if !exists {
		return
	}

	// 在列表中查找并移除消费者
	for i, consumer := range chans {
		if consumer == ch {
			t.consumers[groupName] = append(chans[:i], chans[i+1:]...)
			break // 找到并移除后结束循环
		}
	}

	// 如果组没有消费者了，删除该组
	if len(t.consumers[groupName]) == 0 {
		delete(t.consumers, groupName)
	}
}

// distributeMessage 分发消息给所有消费者组
func (t *Topic) distributeMessage(msg *Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return
	}

	// 为每个消费者组分发消息
	for _, consumerChans := range t.consumers {
		if len(consumerChans) > 0 {
			// 轮询选择消费者（简单负载均衡）
			idx := int(msg.Timestamp.UnixNano()) % len(consumerChans)
			select {
			case consumerChans[idx] <- msg:
				// 消息发送成功
			default:
				// 消费者通道已满，消息将被丢弃
				// 实际应用中可能需要日志记录或重试机制
			}
		}
	}
}

// GetBaseOffset 获取最早可用的偏移量
func (t *Topic) GetBaseOffset() uint64 {
	return t.log.GetBaseOffset()
}

// GetNextOffset 获取下一个可用的偏移量
func (t *Topic) GetNextOffset() uint64 {
	return t.log.GetNextOffset()
}

// cleanupTask 定期清理过期消息
func (t *Topic) cleanupTask() {
	ticker := time.NewTicker(6 * time.Hour) // 每6小时检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 实现简单的基于时间的日志清理
			// 这里仅为示例，实际生产环境可能需要更复杂的策略
			// 目前我们没有在消息中直接存储时间戳，所以这只是一个占位实现
			// 实际实现可能需要维护额外的元数据来跟踪每个偏移量对应的时间
			// t.log.Truncate(someOffset)
		case <-t.ctx.Done():
			return
		}
	}
}

// replayMessagesTask 重放消息给新的消费者
func (t *Topic) replayMessagesTask() {
	// 这个方法可以实现更复杂的功能
	// 目前是一个简单的占位实现
	<-t.ctx.Done() // 等待关闭信号
}

// Close 关闭主题
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	// 发送取消信号
	t.cancel()

	// 关闭所有消费者通道
	for groupName, consumers := range t.consumers {
		for _, ch := range consumers {
			close(ch)
		}
		delete(t.consumers, groupName)
	}

	// 关闭存储日志
	if err := t.log.Close(); err != nil {
		return fmt.Errorf("failed to close storage log: %w", err)
	}

	t.closed = true
	return nil
}
