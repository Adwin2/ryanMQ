package core

import (
	"encoding/json"
	"time"
)

// Message 表示消息队列中的一条消息
type Message struct {
	ID        string            `json:"id"`        // 消息唯一标识符
	Topic     string            `json:"topic"`     // 消息所属主题
	Data      []byte            `json:"data"`      // 消息数据
	Timestamp time.Time         `json:"timestamp"` // 消息创建时间
	Headers   map[string]string `json:"headers"`   // 消息元数据
	Offset    uint64            `json:"offset"`    // 消息在日志中的偏移量
	Partition int               `json:"partition"` // 消息所在分区（预留字段，支持未来扩展）
}

// NewMessage 创建一个新的消息
func NewMessage(topic string, data []byte) *Message {
	return &Message{
		ID:        generateID(),
		Topic:     topic,
		Data:      data,
		Timestamp: time.Now(),
		Headers:   make(map[string]string),
	}
}

// SetHeader 设置消息头部
func (m *Message) SetHeader(key, value string) {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[key] = value
}

// GetHeader 获取消息头部
func (m *Message) GetHeader(key string) (string, bool) {
	if m.Headers == nil {
		return "", false
	}
	value, exists := m.Headers[key]
	return value, exists
}

// MarshalBinary 将消息序列化为二进制格式
func (m *Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalBinary 从二进制格式反序列化消息
func (m *Message) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

// generateID 生成唯一消息ID
func generateID() string {
	// 使用时间纳秒 + 随机数实现简单的唯一ID
	// 实际生产环境可使用UUID等更可靠的方法
	return time.Now().Format("20060102150405.000000000")
}
