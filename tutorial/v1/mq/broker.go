package mq

type Broker struct {
	topics map[string]*Topic
}
