package main

import (
	"fmt"
	"ryanMQ/tutorial/v1/mq"
	"time"
)

func main() {
	broker := mq.NewBroker()

	group := mq.NewConsumerGroup("group1", broker)

	consumer1 := mq.NewConsumer("consumer1", group)
	consumer2 := mq.NewConsumer("consumer2", group)

	if err := consumer1.Subscribe("orders"); err != nil {
		panic(err)
	}
	if err := consumer2.Subscribe("orders"); err != nil {
		panic(err)
	}

	go func() {
		ch := consumer1.Consume()
		for msg := range ch {
			fmt.Printf("Consumer1 received : %s\n", string(msg.Body))
			time.Sleep(500 * time.Millisecond)
		}
	}()

	go func() {
		ch := consumer2.Consume()
		for msg := range ch {
			fmt.Printf("Consumer2 received : %s\n", string(msg.Body))
			time.Sleep(500 * time.Millisecond)
		}
	}()

	producer := mq.NewProducer(broker)

	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Order #%d", i)
		producer.Produce("orders", []byte(message))
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(5 * time.Second)

	consumer1.Close()
	consumer2.Close()
}
