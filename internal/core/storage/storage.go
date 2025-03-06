// Storage 存储层核心接口
package storage

import "ryanMQ/internal/core/protocol"

type Storage interface {
	// 写入消息，返回写入位置
	Append(topic string, partition uint32, messages []protocol.Message) (uint64, error)

	// 读取消息，从指定位置开始返回指定数量的消息
	FetchMessages(topic string, partition uint32, offset uint64, maxCount uint32) ([]protocol.Message, uint64, error)

	// 创建主题和分区
	CreateTopicPartition(topic string, partition uint32) error

	// 删除主题
	DeleteTopic(topic string) error

	// 关闭存储，释放资源
	Close() error
}
