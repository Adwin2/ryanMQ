package protocol

// Message 表示单条消息的结构
type Message struct {
	MsgLen    uint32 // 消息总长度
	Crc32     uint32 // CRC32校验码
	Timestamp uint64 // 消息时间戳
	DataLen   uint32 // 数据长度
	Data      []byte // 消息内容
}

// RequestHeader 请求头部结构
type RequestHeader struct {
	Length          uint32 // 消息总长度
	RequestType     byte   // 请求类型
	CompressionType byte   // 压缩类型
}

// ProduceRequest 生产者请求结构
type ProduceRequest struct {
	Topic     string    // 主题
	Partition uint32    // 分区
	MsgCount  uint32    // 消息数量
	Messages  []Message // 多条消息，支持批量
}

// ProduceResponse 生产者响应结构
type ProduceResponse struct {
	Status byte   // 状态，成功/失败
	Offset uint64 // 写入位置
}

// FetchRequest 消费者拉取请求结构
type FetchRequest struct {
	Topic       string
	Partition   uint32
	StartOffset uint64 // 消费起始位置
	MaxMsgNum   uint32 // 最多消费(拉取)多少条
}

// FetchResponse 消费者拉取响应结构
type FetchResponse struct {
	Status     byte      // 成功/失败
	NextOffset uint64    // 下次拉取的起始位置
	Messages   []Message // 实际返回的消息
}
