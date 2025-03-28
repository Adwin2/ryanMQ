package mq

import (
	"time"

	"github.com/google/uuid"
)

// 一条消息
type Message struct {
	ID        string
	Topic     string
	Body      []byte
	TimeStamp time.Time
}

func NewMessage(topic string, body []byte) *Message {
	return &Message{
		ID:        uuid.New().String(),
		Topic:     topic,
		Body:      body,
		TimeStamp: time.Now(),
	}
}
