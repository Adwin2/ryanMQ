package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/raymond/ryanMQ/tutorial/v2/mq/broker"
	"github.com/raymond/ryanMQ/tutorial/v2/mq/core"
	"github.com/raymond/ryanMQ/tutorial/v2/mq/producer"
)

type SensorData struct {
	SensorID  string    `json:"sensor_id"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// 创建数据目录
	dataDir := "./data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// 创建代理
	brokerInstance := broker.NewBroker(broker.Config{
		DataDir:       dataDir,
		SegmentSize:   100 * 1024 * 1024, // 100MB段大小
		IndexInterval: 100,               // 每100条消息创建一个索引
	})

	// 创建主题
	topicName := "sensors"
	_, err := brokerInstance.CreateTopic(topicName)
	if err != nil {
		log.Printf("Topic creation error (might already exist): %v", err)
	}

	// 创建消费者工厂
	consumerFactory := core.NewConsumerFactory(brokerInstance)

	// 创建消费者组
	consumerGroup := consumerFactory.CreateConsumerGroup(core.ConsumerGroupConfig{
		Name:       "sensor-processors",
		AutoCommit: true,
	})

	// 创建生产者
	sensorProducer, err := producer.NewProducer(
		brokerInstance,
		topicName,
		producer.WithBatchSize(10),             // 每10条消息作为一批
		producer.WithFlushInterval(time.Second), // 每秒自动刷新
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer sensorProducer.Close()

	// 创建上下文用于优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 捕获中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	// 启动多个消费者
	var wg sync.WaitGroup
	consumerCount := 3
	wg.Add(consumerCount)

	for i := 0; i < consumerCount; i++ {
		go func(id int) {
			defer wg.Done()
			runConsumer(ctx, consumerGroup, id)
		}(i)
	}

	// 启动数据生成器
	wg.Add(1)
	go func() {
		defer wg.Done()
		generateData(ctx, sensorProducer)
	}()

	// 等待所有协程结束
	wg.Wait()
	fmt.Println("All workers have completed, shutting down broker...")

	// 关闭代理
	if err := brokerInstance.Close(); err != nil {
		log.Printf("Error closing broker: %v", err)
	}

	fmt.Println("Shutdown complete")
}

// 生成模拟的传感器数据
func generateData(ctx context.Context, p *producer.Producer) {
	sensorIDs := []string{"temp-1", "temp-2", "humid-1", "pressure-1", "wind-1"}
	counter := 0

	fmt.Println("Data generator started")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Data generator stopping...")
			return
		case <-ticker.C:
			// 生成随机传感器数据
			sensorID := sensorIDs[rand.Intn(len(sensorIDs))]
			value := rand.Float64() * 100

			data := SensorData{
				SensorID:  sensorID,
				Value:     value,
				Timestamp: time.Now(),
			}

			// 序列化数据
			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Printf("Error serializing data: %v", err)
				continue
			}

			// 发送消息
			_, err = p.Send(jsonData)
			if err != nil {
				log.Printf("Error sending message: %v", err)
				continue
			}

			counter++
			if counter%100 == 0 {
				fmt.Printf("Generated %d messages\n", counter)
			}
		}
	}
}

// 运行消费者处理消息
func runConsumer(ctx context.Context, cg *core.ConsumerGroup, id int) {
	// 创建消费者
	consumer := core.NewConsumer(fmt.Sprintf("consumer-%d", id), cg)
	defer consumer.Close()

	// 订阅主题
	if err := consumer.Subscribe("sensors"); err != nil {
		log.Printf("Consumer %d: Error subscribing to topic: %v", id, err)
		return
	}

	fmt.Printf("Consumer %d started\n", id)

	// 处理消息
	messagesProcessed := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Consumer %d stopping... (processed %d messages)\n", id, messagesProcessed)
			return
		default:
			// 尝试消费消息
			msg, err := consumer.Consume(500 * time.Millisecond)
			if err != nil {
				// 超时是正常的，忽略
				continue
			}

			// 解析消息
			var sensorData SensorData
			if err := json.Unmarshal(msg.Data, &sensorData); err != nil {
				log.Printf("Consumer %d: Error unmarshaling message: %v", id, err)
				continue
			}

			// 处理消息（这里只是打印）
			messagesProcessed++
			if messagesProcessed%100 == 0 {
				fmt.Printf("Consumer %d: Processed %d messages, latest: %s=%.2f\n",
					id, messagesProcessed, sensorData.SensorID, sensorData.Value)
			}

			// 模拟处理时间
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		}
	}
}
