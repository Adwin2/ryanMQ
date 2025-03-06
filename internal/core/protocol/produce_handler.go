/*
处理生产者请求

批量写入消息
返回写入成功的Offset
*/
package protocol

type ProduceHandler struct {
	storage *StorageHandler
	metrics *Metrics
}
