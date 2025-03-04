/*
消息编解码

定义二进制协议格式(参考Kafka)
处理压缩(Snappy)
*/
package protocol

import (
	"bytes"
	"encoding/binary"
	"ryanMQ/internal/utils/log"

	"github.com/golang/snappy"
)

const (
	ProduceRequestType       = byte(0x01)
	FetchRequestType         = byte(0x02)
	OffsetCommitType         = byte(0x03)
	TopicMetadataRequestType = byte(0x04)
)

func EncodeProduceRequest(req *ProduceRequest) ([]byte, error) {
	buf := new(bytes.Buffer) //写入缓冲区

	//主题 ：2 字节长度(int16类型) + 数据
	topicBytes := []byte(req.Topic)
	//大端字节序存储
	if err := binary.Write(buf, binary.BigEndian, int16(len(topicBytes))); err != nil {
		return nil, err
	}
	buf.Write(topicBytes)

	//分区号：4字节
	binary.Write(buf, binary.BigEndian, req.Partition)

	//消息列表 (每条消息： 4字节 长度(len) + 数据)
	for _, msg := range req.Messages {
		binary.Write(buf, binary.BigEndian, int32(len(msg)))
		buf.Write(msg)
	}

	//构建最终请求（添加请求头和长度）
	header := RequestHeader{
		Length:      int32(buf.Len()),
		RequestType: ProduceRequestType, //生产者请求
	}
	headerBytes, err := encodeHeader(header)
	if err != nil {
		log.Error("encode header error: %v", err)
		return nil, err
	}

	return append(headerBytes, buf.Bytes()...), nil
}

func encodeHeader(header RequestHeader) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, header.Length); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, header.RequestType); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeProduceRequest(data []byte) (*ProduceRequest, error) {
	buf := bytes.NewBuffer(data)
	req := &ProduceRequest{}

	//解析请求头

}

func EncodeConsumeRequest()

func CompressWithSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func DecompressWithSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
