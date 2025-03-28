package core

import (
	"errors"
	"fmt"
)

// Broker 消息队列的顶层接口
type Broker interface {
	// CreateTopic 创建新主题
	CreateTopic(name string) (*Topic, error)
	
	// GetTopic 获取主题
	GetTopic(name string) (*Topic, error)
	
	// ListTopics 获取所有主题名称
	ListTopics() []string
	
	// DeleteTopic 删除主题
	DeleteTopic(name string) error
	
	// PublishMessage 发布消息到指定主题
	PublishMessage(topicName string, data []byte) (*Message, error)
	
	// Close 关闭代理，清理资源
	Close() error
}

// BatchPublishResult 批量发布结果
type BatchPublishResult struct {
	Messages []*Message
	Errors   []error
}

// BatchPublisher 批量发布接口
type BatchPublisher interface {
	// BatchPublish 批量发布消息到指定主题
	BatchPublish(topicName string, dataList [][]byte) BatchPublishResult
}

// 实现批量发布接口的适配器
type batchPublishAdapter struct {
	broker Broker
}

// NewBatchPublisher 创建一个批量发布适配器
func NewBatchPublisher(broker Broker) BatchPublisher {
	return &batchPublishAdapter{broker: broker}
}

// BatchPublish 批量发布消息的实现
func (b *batchPublishAdapter) BatchPublish(topicName string, dataList [][]byte) BatchPublishResult {
	result := BatchPublishResult{
		Messages: make([]*Message, 0, len(dataList)),
		Errors:   make([]error, 0),
	}

	for _, data := range dataList {
		msg, err := b.broker.PublishMessage(topicName, data)
		if err != nil {
			result.Messages = append(result.Messages, nil)
			result.Errors = append(result.Errors, err)
		} else {
			result.Messages = append(result.Messages, msg)
			result.Errors = append(result.Errors, nil)
		}
	}

	return result
}

// ConsumerFactory 创建消费者接口
type ConsumerFactory interface {
	// CreateConsumerGroup 创建消费者组
	CreateConsumerGroup(config ConsumerGroupConfig) *ConsumerGroup
	
	// GetConsumerGroup 获取指定名称的消费者组
	GetConsumerGroup(name string) (*ConsumerGroup, error)
}

// OffsetResetStrategy 偏移量重置策略
type OffsetResetStrategy int

const (
	// OffsetEarliest 从最早的消息开始消费
	OffsetEarliest OffsetResetStrategy = iota
	
	// OffsetLatest 从最新的消息开始消费
	OffsetLatest
	
	// OffsetCommitted 从最后提交的偏移量开始消费
	OffsetCommitted
)

// ConsumerFactoryImpl 实现消费者工厂接口
type ConsumerFactoryImpl struct {
	broker         Broker
	consumerGroups map[string]*ConsumerGroup
}

// NewConsumerFactory 创建消费者工厂
func NewConsumerFactory(broker Broker) ConsumerFactory {
	return &ConsumerFactoryImpl{
		broker:         broker,
		consumerGroups: make(map[string]*ConsumerGroup),
	}
}

// CreateConsumerGroup 创建消费者组
func (cf *ConsumerFactoryImpl) CreateConsumerGroup(config ConsumerGroupConfig) *ConsumerGroup {
	group := NewConsumerGroup(cf.broker.(*brokerImpl), config)
	cf.consumerGroups[config.Name] = group
	return group
}

// GetConsumerGroup 获取指定名称的消费者组
func (cf *ConsumerFactoryImpl) GetConsumerGroup(name string) (*ConsumerGroup, error) {
	group, exists := cf.consumerGroups[name]
	if !exists {
		return nil, fmt.Errorf("consumer group %s not found", name)
	}
	return group, nil
}

// brokerImpl 是 Broker 接口的基本实现
type brokerImpl struct {
	topics  map[string]*Topic
	dataDir string
}

// NewBroker 创建一个新的消息代理
func NewBroker(dataDir string) Broker {
	return &brokerImpl{
		topics:  make(map[string]*Topic),
		dataDir: dataDir,
	}
}

// CreateTopic 创建新主题
func (b *brokerImpl) CreateTopic(name string) (*Topic, error) {
	// 检查主题是否已存在
	if _, exists := b.topics[name]; exists {
		return nil, errors.New("topic already exists")
	}

	// 创建主题配置
	config := TopicConfig{
		Name:       name,
		StorageDir: fmt.Sprintf("%s/topics/%s", b.dataDir, name),
	}

	// 创建主题
	topic, err := NewTopic(config)
	if err != nil {
		return nil, err
	}

	b.topics[name] = topic
	return topic, nil
}

// GetTopic 获取主题
func (b *brokerImpl) GetTopic(name string) (*Topic, error) {
	topic, exists := b.topics[name]
	if !exists {
		return nil, errors.New("topic not found")
	}

	return topic, nil
}

// ListTopics 获取所有主题名称
func (b *brokerImpl) ListTopics() []string {
	topicNames := make([]string, 0, len(b.topics))
	for name := range b.topics {
		topicNames = append(topicNames, name)
	}

	return topicNames
}

// DeleteTopic 删除主题
func (b *brokerImpl) DeleteTopic(name string) error {
	topic, exists := b.topics[name]
	if !exists {
		return errors.New("topic not found")
	}

	if err := topic.Close(); err != nil {
		return err
	}

	delete(b.topics, name)
	return nil
}

// PublishMessage 发布消息到指定主题
func (b *brokerImpl) PublishMessage(topicName string, data []byte) (*Message, error) {
	// 获取主题
	topic, err := b.GetTopic(topicName)
	if err != nil {
		// 如果主题不存在，则创建它
		topic, err = b.CreateTopic(topicName)
		if err != nil {
			return nil, err
		}
	}

	// 发布消息
	msg, err := topic.Publish(data)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Close 关闭代理，清理资源
func (b *brokerImpl) Close() error {
	var lastErr error
	for name, topic := range b.topics {
		if err := topic.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close topic %s: %w", name, err)
		}
	}

	return lastErr
}
