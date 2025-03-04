package protocol

type RequestHeader struct {
	Length      int32 //消息长度
	RequestType byte  //请求类型
}

// 生产者请求
type ProduceRequest struct {
	Topic     string
	Partition int32
	Messages  [][]byte //多条消息， 支持批量
}

type ProduceResponse struct {
	Status byte  //成功/失败
	Offset int64 //写入的Offset
}

type FetchRequest struct {
	Topic       string
	Partition   int32
	StartOffset int64 //消费起始位置
	MaxMsgNum   int32 //最多消费(拉取)多少条
}

type FetchResponse struct {
	Status     byte     //成功/失败
	NextOffset int64    //下次拉取的起始位置
	Messages   [][]byte //实际返回的消息
}
