package mq

import "sync"

type Broker struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

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

	topic := NewTopic
}
