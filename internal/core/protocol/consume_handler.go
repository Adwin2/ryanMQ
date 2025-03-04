/*
 处理消费者请求

 根据Offset读取消息
 支持长轮询 (Long Polling)

 ​关键技术：
 基于 io.Reader/io.Writer 实现协议解析
 用 goroutine 池处理并发请求
*/