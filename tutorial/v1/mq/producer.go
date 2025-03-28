package mq

// 消息生产者客户端
type Producer struct {
	broker *Broker
}

func NewProducer(broker *Broker) *Producer {
	return &Producer{
		broker: broker,
	}
}

func (p *Producer) Produce(topic string, data []byte) error {
	return p.broker.publishToTopic(topic, data)
}
