package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/raymond/ryanMQ/tutorial/v2/mq/core"
)

var (
	ErrTopicNotFound = errors.New("topic not found")
	ErrTopicExists   = errors.New("topic already exists")
)

// Broker 消息队列的中央管理器
type Broker struct {
	topics      map[string]*core.Topic
	mu          sync.RWMutex
	dataDir     string
	topicConfig core.TopicConfig
}

// Config 代理配置
type Config struct {
	DataDir      string // 数据存储目录
	SegmentSize  uint64 // 段大小
	IndexInterval uint64 // 索引间隔
}

// NewBroker 创建一个新的消息代理
func NewBroker(config Config) *Broker {
	// 设置默认值
	if config.DataDir == "" {
		config.DataDir = "data"
	}
	if config.SegmentSize == 0 {
		config.SegmentSize = 1024 * 1024 * 1024 // 默认1GB
	}
	if config.IndexInterval == 0 {
		config.IndexInterval = 100 // 默认每100条消息建立索引
	}

	return &Broker{
		topics:  make(map[string]*core.Topic),
		dataDir: config.DataDir,
		topicConfig: core.TopicConfig{
			SegmentMaxSize: config.SegmentSize,
			IndexInterval:  config.IndexInterval,
		},
	}
}

// CreateTopic 创建新主题
func (b *Broker) CreateTopic(name string) (*core.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 检查主题是否已存在
	if _, exists := b.topics[name]; exists {
		return nil, ErrTopicExists
	}

	// 设置主题配置
	topicConfig := b.topicConfig
	topicConfig.Name = name
	topicConfig.StorageDir = filepath.Join(b.dataDir, "topics", name)

	// 创建主题
	topic, err := core.NewTopic(topicConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create topic: %w", err)
	}

	// 保存主题
	b.topics[name] = topic
	return topic, nil
}

// GetTopic 获取主题
func (b *Broker) GetTopic(name string) (*core.Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, exists := b.topics[name]
	if !exists {
		return nil, ErrTopicNotFound
	}

	return topic, nil
}

// ListTopics 获取所有主题名称
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topicNames := make([]string, 0, len(b.topics))
	for name := range b.topics {
		topicNames = append(topicNames, name)
	}

	return topicNames
}

// DeleteTopic 删除主题
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic, exists := b.topics[name]
	if !exists {
		return ErrTopicNotFound
	}

	// 关闭主题
	if err := topic.Close(); err != nil {
		return fmt.Errorf("failed to close topic: %w", err)
	}

	// 从映射中删除主题
	delete(b.topics, name)

	return nil
}

// PublishMessage 发布消息到指定主题
func (b *Broker) PublishMessage(topicName string, data []byte) (*core.Message, error) {
	// 获取主题
	topic, err := b.GetTopic(topicName)
	if err != nil {
		// 如果主题不存在，创建一个新的
		topic, err = b.CreateTopic(topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic: %w", err)
		}
	}

	// 发布消息
	msg, err := topic.Publish(data)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}

	return msg, nil
}

// Close 关闭代理，清理资源
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var lastErr error
	for name, topic := range b.topics {
		if err := topic.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close topic %s: %w", name, err)
		}
		delete(b.topics, name)
	}

	return lastErr
}
