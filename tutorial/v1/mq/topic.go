package mq

import "sync"

// 消息主题
type Topic struct {
	name      string
	msgChan   chan *Message
	consumers map[string][]chan *Message //消费者组到消费者channel的映射
	mu        sync.RWMutex
}

func NewTopic(name string) *Topic {
	t := &Topic{
		name:      name,
		msgChan:   make(chan *Message, 100), //缓冲区大小随需
		consumers: make(map[string][]chan *Message),
	}

	go t.distributeMessages()

	return t
}

func (t *Topic) distributeMessages() {
	for msg := range t.msgChan {
		t.mu.RLock()
		//向每个消费者组发送消息副本
		for _, consumerChans := range t.consumers {
			if len(consumerChans) > 0 {
				//轮询选择消费者     实际可能需要负载均衡策略
				idx := int(msg.TimeStamp.UnixNano()) % len(consumerChans)
				select {
				case consumerChans[idx] <- msg:
					//done
				default:
					//消费者channel已满，记录日志或其他措施
				}
			}
		}
		t.mu.RUnlock()
	}
}

// Publish 向主题发布一条消息
func (t *Topic) Publish(msg *Message) {
	t.msgChan <- msg
}

// AddConsumer 添加一个消费者到指定的消费者组
func (t *Topic) AddConsumer(groupName string, consumerChan chan *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.consumers[groupName]; !exists {
		t.consumers[groupName] = make([]chan *Message, 0)
	}

	t.consumers[groupName] = append(t.consumers[groupName], consumerChan)
}

// RemoveConsumer 从指定的消费者组移除一个消费者
func (t *Topic) RemoveConsumer(groupName string, consumerChan chan *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if chans, exists := t.consumers[groupName]; exists {
		for i, ch := range chans {
			if ch == consumerChan {
				// 移除消费者channel 、错过chans[i]
				// ... 使chans[i+1:]以每个元素的形式加到前者后
				t.consumers[groupName] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
	}
}
