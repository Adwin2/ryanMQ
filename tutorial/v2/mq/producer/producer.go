package producer

import (
	"errors"
	"sync"
	"time"

	"github.com/raymond/ryanMQ/tutorial/v2/mq/core"
)

// Producer 表示生产者，用于向主题发送消息
type Producer struct {
	broker      core.Broker
	topicName   string
	batchSize   int
	batchBuffer [][]byte
	bufferLock  sync.Mutex
	flushTicker *time.Ticker
	stopChan    chan struct{}
	closed      bool
}

// ProducerOption 生产者配置选项
type ProducerOption func(*Producer)

// WithBatchSize 设置批处理大小
func WithBatchSize(size int) ProducerOption {
	return func(p *Producer) {
		p.batchSize = size
	}
}

// WithFlushInterval 设置自动刷新间隔
func WithFlushInterval(interval time.Duration) ProducerOption {
	return func(p *Producer) {
		if p.flushTicker != nil {
			p.flushTicker.Stop()
		}
		p.flushTicker = time.NewTicker(interval)
	}
}

// NewProducer 创建一个新的生产者
func NewProducer(broker core.Broker, topicName string, options ...ProducerOption) (*Producer, error) {
	if broker == nil {
		return nil, errors.New("broker cannot be nil")
	}
	if topicName == "" {
		return nil, errors.New("topic name cannot be empty")
	}

	// 确保主题存在
	_, err := broker.GetTopic(topicName)
	if err != nil {
		// 如果主题不存在，则创建它
		_, err = broker.CreateTopic(topicName)
		if err != nil {
			return nil, err
		}
	}

	producer := &Producer{
		broker:      broker,
		topicName:   topicName,
		batchSize:   100,   // 默认批处理大小
		batchBuffer: make([][]byte, 0, 100),
		stopChan:    make(chan struct{}),
	}

	// 应用选项
	for _, option := range options {
		option(producer)
	}

	// 如果没有设置刷新间隔，则默认为1秒
	if producer.flushTicker == nil {
		producer.flushTicker = time.NewTicker(time.Second)
	}

	// 启动后台刷新协程
	go producer.flushLoop()

	return producer, nil
}

// Send 发送单条消息
func (p *Producer) Send(data []byte) (*core.Message, error) {
	if p.closed {
		return nil, errors.New("producer is closed")
	}

	// 如果启用了批处理并且消息足够小
	if p.batchSize > 1 && len(data) < 1024*1024 { // 1MB限制
		return p.batchSend(data)
	}

	// 直接发送大消息
	return p.broker.PublishMessage(p.topicName, data)
}

// batchSend 将消息添加到批处理缓冲区
func (p *Producer) batchSend(data []byte) (*core.Message, error) {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()

	// 复制数据到新的缓冲区
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	// 添加到批处理缓冲区
	p.batchBuffer = append(p.batchBuffer, dataCopy)

	// 如果缓冲区已满，立即刷新
	if len(p.batchBuffer) >= p.batchSize {
		return p.flushBatchBuffer()
	}

	// 返回 nil 表示消息已加入批处理队列
	// 在实际应用中，可能需要使用更复杂的方式来跟踪批处理结果
	return nil, nil
}

// flushBatchBuffer 刷新批处理缓冲区
func (p *Producer) flushBatchBuffer() (*core.Message, error) {
	if len(p.batchBuffer) == 0 {
		return nil, nil
	}

	// 获取当前批次并清空缓冲区
	currentBatch := p.batchBuffer
	p.batchBuffer = make([][]byte, 0, p.batchSize)

	// 创建批量发布适配器
	batchPublisher := core.NewBatchPublisher(p.broker)
	result := batchPublisher.BatchPublish(p.topicName, currentBatch)

	// 检查错误
	for _, err := range result.Errors {
		if err != nil {
			return nil, err // 返回第一个错误
		}
	}

	// 返回最后一条消息
	if len(result.Messages) > 0 {
		return result.Messages[len(result.Messages)-1], nil
	}

	return nil, nil
}

// flushLoop 定期刷新批处理缓冲区
func (p *Producer) flushLoop() {
	for {
		select {
		case <-p.flushTicker.C:
			p.Flush()
		case <-p.stopChan:
			return
		}
	}
}

// Flush 手动刷新批处理缓冲区
func (p *Producer) Flush() error {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()

	_, err := p.flushBatchBuffer()
	return err
}

// Close 关闭生产者
func (p *Producer) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true
	p.flushTicker.Stop()
	close(p.stopChan)

	// 刷新剩余消息
	return p.Flush()
}
