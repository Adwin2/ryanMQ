package mq

import (
	"errors"
	"sync"
)

// 消息队列的中央管理器
type Broker struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

// 创建一个新的消息代理
func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
	}
}

func (b *Broker) CreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if topic, exists := b.topics[name]; exists {
		return topic
	}

	topic := NewTopic(name)
	b.topics[name] = topic
	return topic
}

// 获取指定名称的主题
func (b *Broker) GetTopic(name string) (*Topic, error) {
	// b.mu.Lock()
	// defer b.mu.Unlock()

	topic, exists := b.topics[name]
	if !exists {
		return nil, errors.New("topic not found")
	}

	return topic, nil
}

// publishToTopic 想指定主题发布消息
func (b *Broker) publishToTopic(topicName string, data []byte) error {
	topic, err := b.GetTopic(topicName)
	if err != nil {
		// 主题不存在，创建一个新的
		topic = b.CreateTopic(topicName)
		if topic == nil {
			return errors.New("failed to create topic")
		}
	}

	msg := NewMessage(topicName, data)
	topic.Publish(msg)
	return nil
}
