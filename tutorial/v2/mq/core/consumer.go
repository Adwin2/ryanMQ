package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrConsumerClosed = errors.New("consumer is closed")
)

// ConsumerGroup 表示一组消费者实例
type ConsumerGroup struct {
	name       string
	broker     *Broker
	mu         sync.RWMutex
	consumers  map[string]*Consumer // 消费者ID -> 消费者实例
	topicSubs  map[string]struct{}  // 已订阅的主题名称
	offset     map[string]uint64    // 主题 -> 当前处理的偏移量
	autoCommit bool                 // 是否自动提交偏移量
}

// ConsumerGroupConfig 消费者组配置
type ConsumerGroupConfig struct {
	Name       string
	AutoCommit bool
}

// NewConsumerGroup 创建一个新的消费者组
func NewConsumerGroup(broker *Broker, config ConsumerGroupConfig) *ConsumerGroup {
	if config.Name == "" {
		config.Name = "default-group"
	}

	return &ConsumerGroup{
		name:       config.Name,
		broker:     broker,
		consumers:  make(map[string]*Consumer),
		topicSubs:  make(map[string]struct{}),
		offset:     make(map[string]uint64),
		autoCommit: config.AutoCommit,
	}
}

// Subscribe 订阅主题
func (cg *ConsumerGroup) Subscribe(topicName string) error {
	// 获取或创建主题
	topic, err := cg.broker.GetTopic(topicName)
	if err != nil {
		topic, err = cg.broker.CreateTopic(topicName)
		if err != nil {
			return err
		}
	}

	cg.mu.Lock()
	// 记录订阅主题
	cg.topicSubs[topicName] = struct{}{}
	
	// 初始化偏移量（从最新消息开始消费）
	if _, exists := cg.offset[topicName]; !exists {
		cg.offset[topicName] = topic.GetNextOffset()
	}
	cg.mu.Unlock()

	return nil
}

// IsSubscribed 检查是否已订阅主题
func (cg *ConsumerGroup) IsSubscribed(topicName string) bool {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	
	_, subscribed := cg.topicSubs[topicName]
	return subscribed
}

// SetOffset 设置主题的消费偏移量
func (cg *ConsumerGroup) SetOffset(topicName string, offset uint64) error {
	if !cg.IsSubscribed(topicName) {
		return errors.New("topic not subscribed")
	}

	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.offset[topicName] = offset
	return nil
}

// GetOffset 获取主题的当前消费偏移量
func (cg *ConsumerGroup) GetOffset(topicName string) (uint64, error) {
	if !cg.IsSubscribed(topicName) {
		return 0, errors.New("topic not subscribed")
	}

	cg.mu.RLock()
	defer cg.mu.RUnlock()
	offset, exists := cg.offset[topicName]
	if !exists {
		return 0, errors.New("offset not set for topic")
	}
	return offset, nil
}

// CommitOffset 提交偏移量
func (cg *ConsumerGroup) CommitOffset(topicName string, offset uint64) error {
	if !cg.IsSubscribed(topicName) {
		return errors.New("topic not subscribed")
	}

	cg.mu.Lock()
	defer cg.mu.Unlock()
	// 保存偏移量（在实际系统中，这里会持久化到某种存储中）
	cg.offset[topicName] = offset
	return nil
}

// AddConsumer 将新的消费者添加到组
func (cg *ConsumerGroup) AddConsumer(consumer *Consumer) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.consumers[consumer.id] = consumer
}

// RemoveConsumer 从组中移除消费者
func (cg *ConsumerGroup) RemoveConsumer(consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.consumers, consumerID)
}

// Consumer 单个消费者实例
type Consumer struct {
	id        string
	group     *ConsumerGroup
	msgChan   chan *Message // 接收消息的channel
	topics    map[string]struct{}
	topicsMu  sync.RWMutex
	ctx       context.Context    // 用于管理消费者生命周期
	cancel    context.CancelFunc // 用于取消context
	closed    bool
}

// NewConsumer 创建一个新的消费者
func NewConsumer(id string, group *ConsumerGroup) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		id:      id,
		group:   group,
		msgChan: make(chan *Message, 50), // 缓冲区大小可调整
		topics:  make(map[string]struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}
	group.AddConsumer(consumer)
	return consumer
}

// Subscribe 订阅主题
func (c *Consumer) Subscribe(topicName string) error {
	// 确保消费者组已订阅该主题
	if !c.group.IsSubscribed(topicName) {
		if err := c.group.Subscribe(topicName); err != nil {
			return err
		}
	}

	// 获取主题
	topic, err := c.group.broker.GetTopic(topicName)
	if err != nil {
		return err
	}

	c.topicsMu.Lock()
	defer c.topicsMu.Unlock()

	// 避免重复订阅
	if _, exists := c.topics[topicName]; exists {
		return nil
	}

	// 将消费者的channel添加到主题
	topic.AddConsumer(c.group.name, c.msgChan)
	c.topics[topicName] = struct{}{}

	// 启动消费协程
	go c.consumeFromTopic(topicName)

	return nil
}

// Unsubscribe 取消订阅主题
func (c *Consumer) Unsubscribe(topicName string) error {
	// 获取主题
	topic, err := c.group.broker.GetTopic(topicName)
	if err != nil {
		return err
	}

	c.topicsMu.Lock()
	defer c.topicsMu.Unlock()

	if _, exists := c.topics[topicName]; exists {
		topic.RemoveConsumer(c.group.name, c.msgChan)
		delete(c.topics, topicName)
	}

	return nil
}

// Consume 从消息通道获取下一条消息
func (c *Consumer) Consume(timeout time.Duration) (*Message, error) {
	if c.closed {
		return nil, ErrConsumerClosed
	}

	if timeout == 0 {
		// 无超时，直接等待消息
		select {
		case msg := <-c.msgChan:
			return msg, nil
		case <-c.ctx.Done():
			return nil, ErrConsumerClosed
		}
	} else {
		// 带超时的等待
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case msg := <-c.msgChan:
			return msg, nil
		case <-timer.C:
			return nil, errors.New("consume timeout")
		case <-c.ctx.Done():
			return nil, ErrConsumerClosed
		}
	}
}

// Context 返回消费者的上下文
func (c *Consumer) Context() context.Context {
	return c.ctx
}

// consumeFromTopic 从持久化存储消费主题消息
func (c *Consumer) consumeFromTopic(topicName string) {
	offset, err := c.group.GetOffset(topicName)
	if err != nil {
		// 错误处理
		return
	}

	topic, err := c.group.broker.GetTopic(topicName)
	if err != nil {
		// 错误处理
		return
	}

	nextOffset := offset
	for {
		select {
		case <-c.ctx.Done():
			return // 消费者被关闭
		default:
			// 继续处理
		}

		// 尝试读取消息
		msg, err := topic.Read(nextOffset)
		if err != nil {
			// 如果没有更多消息或其他错误，等待一会再试
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 发送消息到消费通道
		select {
		case c.msgChan <- msg:
			// 消息已发送
			nextOffset++
			
			// 如果自动提交偏移量，提交当前偏移量
			if c.group.autoCommit {
				c.group.CommitOffset(topicName, nextOffset)
			}
			
		case <-c.ctx.Done():
			return // 消费者被关闭
		}
	}
}

// CommitOffset 手动提交偏移量
func (c *Consumer) CommitOffset(topicName string, offset uint64) error {
	return c.group.CommitOffset(topicName, offset)
}

// Close 关闭消费者
func (c *Consumer) Close() {
	if c.closed {
		return
	}

	// 发送取消信号
	c.cancel()

	// 取消所有主题订阅
	c.topicsMu.Lock()
	for topicName := range c.topics {
		topic, _ := c.group.broker.GetTopic(topicName)
		if topic != nil {
			topic.RemoveConsumer(c.group.name, c.msgChan)
		}
	}
	c.topics = make(map[string]struct{})
	c.topicsMu.Unlock()

	// 从消费者组移除
	c.group.RemoveConsumer(c.id)

	// 关闭消息channel
	close(c.msgChan)
	c.closed = true
}
