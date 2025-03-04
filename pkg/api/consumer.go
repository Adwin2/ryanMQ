/*
consumer.go：消费者接口
Subscribe(topic string) <-chan Message
Ack(offset int) 手动提交 Offset

关键技术：
基于 net.Dial 实现客户端通信
用 channel 实现消息推送
*/