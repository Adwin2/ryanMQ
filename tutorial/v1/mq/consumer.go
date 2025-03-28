package mq

import (
	"context"
	"errors"
	"sync"
)

type ConsumerGroup struct {
	name      string
	broker    *Broker
	topicSubs map[string]struct{} // 已经订阅的主题集合
	mu        sync.RWMutex
}

func NewConsumerGroup(name string, broker *Broker) *ConsumerGroup {
	return &ConsumerGroup{
		name:      name,
		broker:    broker,
		topicSubs: make(map[string]struct{}),
	}
}

// 订阅指定主题
func (cg *ConsumerGroup) Subscribe(topicName string) error {
	topic, err := cg.broker.GetTopic(topicName)
	if err != nil {
		// 主题不存在，创建一个新的
		topic = cg.broker.CreateTopic(topicName)
		if topic == nil {
			return errors.New("failed to create topic")
		}
	}

	cg.mu.Lock()
	//struct{}{} 类型＋空的内容 ：使用不占内存的空结构体 作为存在标识  --> map[key]struct{}
	cg.topicSubs[topicName] = struct{}{}
	cg.mu.Unlock()

	return nil
}

// IsSubscribed 检查是否已订阅主题
func (cg *ConsumerGroup) IsSubscribed(topicName string) bool {
	_, subscribed := cg.topicSubs[topicName]
	return subscribed
}

/////////////////////////////////////////////////////////////////////////

// Consumer 单个消费者实例
type Consumer struct {
	id       string
	group    *ConsumerGroup
	msgChan  chan *Message // 接收消息的channel
	topics   map[string]struct{}
	topicsMu sync.RWMutex
	ctx      context.Context    // 用于管理消费者生命周期
	cancel   context.CancelFunc // 用于取消context
}

// NewConsumer 创建一个新的消费者
func NewConsumer(id string, group *ConsumerGroup) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		id:      id,
		group:   group,
		msgChan: make(chan *Message, 50), // 缓冲区大小可调整
		topics:  make(map[string]struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Subscribe 订阅指定主题
func (c *Consumer) Subscribe(topicName string) error {
	// c.topicsMu.Lock()
	// defer c.topicsMu.Unlock()
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

	return nil
}

// Unsubscribe 取消订阅指定主题
func (c *Consumer) Unsubscribe(topicName string) error {
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

// Consume 开始消费消息，返回一个用于接收消息的channel
func (c *Consumer) Consume() <-chan *Message {
	return c.msgChan
}

// Context 返回消费者的context，可用于监听取消信号
func (c *Consumer) Context() context.Context {
	return c.ctx
}

// Close 关闭消费者
func (c *Consumer) Close() {
	// 发送取消信号
	c.cancel()

	// 取消所有主题订阅
	c.topicsMu.Lock()
	for topicName := range c.topics {
		if topic, err := c.group.broker.GetTopic(topicName); err == nil {
			topic.RemoveConsumer(c.group.name, c.msgChan)
		}
	}
	c.topics = make(map[string]struct{})
	c.topicsMu.Unlock()

	// 关闭消息channel
	close(c.msgChan)
}
