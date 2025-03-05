package protocol

type Message struct {
	msg_len   uint32 //消息长度
	CRC       uint32 //校验和
	timestamp uint64 //时间戳
	data_len  uint32 //数据长度
	data      []byte //数据
}

type RequestHeader struct {
	Length          uint32 //消息长度
	RequestType     byte   //请求类型
	CompressionType byte   //压缩类型
}

// 生产者请求
type ProduceRequest struct {
	Topic     string
	Partition int32
	msg_cnt   uint32    //???????????????
	Messages  []Message //多条消息， 支持批量
}

type ProduceResponse struct {
	Status byte  //成功/失败
	Offset int64 //写入的Offset
}

type FetchRequest struct {
	Topic       string
	Partition   uint32
	StartOffset uint64 //消费起始位置
	MaxMsgNum   uint32 //最多消费(拉取)多少条
}

type FetchResponse struct {
	Status     byte      //成功/失败
	NextOffset uint64    //下次拉取的起始位置
	Messages   []Message //实际返回的消息
}
