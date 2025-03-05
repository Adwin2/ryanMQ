package protocol__test

import (
	"fmt"
)

type Message struct {
	msg_len   uint32
	CRC       uint32
	timestamp uint32
	data_len  uint32
	data      []byte
}

func main() {
	msg := Message{
		msg_len:   0, // 这里可以初始化
		CRC:       0,
		timestamp: 0,
		data_len:  0,
		data:      []byte("Hello, World!"),
	}

	// 设置 data_len
	msg.data_len = uint32(len(msg.data))
	msg.msg_len = msg.data_len + 12 // 例如，假设 msg_len 包括其他字段的大小

	fmt.Printf("Message Length: %d\n", msg.msg_len)
	fmt.Printf("Data Length: %d\n", msg.data_len)
}
