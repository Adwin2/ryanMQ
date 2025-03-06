package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"ryanMQ/internal/utils/rlog"

	//"ryanMQ/internal/utils/rlog"
	"sync"

	"github.com/golang/snappy"
)

const (
	// 请求类型常量
	ProduceRequestType       = byte(0x01)
	FetchRequestType         = byte(0x02)
	OffsetCommitType         = byte(0x03)
	TopicMetadataRequestType = byte(0x04)

	// 压缩类型常量
	CompressionTypeNone   = byte(0x00)
	CompressionTypeSnappy = byte(0x01)

	// 响应状态常量
	StatusSuccess = byte(0x00)
	StatusError   = byte(0x01)
)

// 全局CRC32表，用于加速CRC计算
var crcTable = crc32.MakeTable(crc32.IEEE)

// EncodeProduceRequest 编码生产请求
func EncodeProduceRequest(req *ProduceRequest) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// 主题: 2字节长度(uint16类型) + 数据
	topicBytes := []byte(req.Topic)
	if err := binary.Write(buf, binary.BigEndian, uint16(len(topicBytes))); err != nil {
		return nil, err
	}
	buf.Write(topicBytes)

	// 分区号
	if err := binary.Write(buf, binary.BigEndian, req.Partition); err != nil {
		return nil, err
	}

	// 消息数量
	if err := binary.Write(buf, binary.BigEndian, req.MsgCount); err != nil {
		return nil, err
	}

	// 消息列表
	for _, msg := range req.Messages {
		// 编码单条消息
		msgBytes, err := encodeMessage(&msg)
		if err != nil {
			return nil, err
		}
		buf.Write(msgBytes)
	}

	// 生成请求头
	header := RequestHeader{
		Length:          uint32(buf.Len()),
		RequestType:     ProduceRequestType,
		CompressionType: CompressionTypeNone, // 默认不压缩
	}

	// 编码请求头
	headerBytes, err := encodeHeader(header)
	if err != nil {
		return nil, err
	}

	return append(headerBytes, buf.Bytes()...), nil
}

// 编码请求头
func encodeHeader(header RequestHeader) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	binary.Write(buf, binary.BigEndian, header.Length)
	binary.Write(buf, binary.BigEndian, header.RequestType)
	binary.Write(buf, binary.BigEndian, header.CompressionType)

	return buf.Bytes(), nil
}

// 编码单条消息
func encodeMessage(msg *Message) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// 写入消息长度
	if err := binary.Write(buf, binary.BigEndian, msg.MsgLen); err != nil {
		return nil, err
	}

	// 写入时间戳
	if err := binary.Write(buf, binary.BigEndian, msg.Timestamp); err != nil {
		return nil, err
	}

	// 写入数据长度
	if err := binary.Write(buf, binary.BigEndian, msg.DataLen); err != nil {
		return nil, err
	}

	// 写入数据
	buf.Write(msg.Data)

	// 计算并写入CRC32
	bytes := buf.Bytes()
	crc := crc32.Checksum(bytes[8:], crcTable) // 跳过MsgLen和Crc32字段

	// 在第4-8字节位置写入CRC32值
	binary.BigEndian.PutUint32(bytes[4:8], crc)

	return bytes, nil
}

// VerifyMessages 验证消息列表的CRC校验码
func VerifyMessages(messages []Message) (bool, error) {
	for _, msg := range messages {
		// 重新计算CRC
		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer bufferPool.Put(buf)

		// 写入字段
		binary.Write(buf, binary.BigEndian, msg.Timestamp)
		binary.Write(buf, binary.BigEndian, msg.DataLen)
		buf.Write(msg.Data)

		// 计算CRC32
		calculatedCRC := crc32.Checksum(buf.Bytes(), crcTable)

		if msg.Crc32 != calculatedCRC {
			rlog.Error("Msg CRC check failed")
			return false, errors.New("CRC check failed for message")
		}
	}

	return true, nil
}

// EncodeFetchRequest 编码拉取请求
func EncodeFetchRequest(req *FetchRequest) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// 主题: 2字节长度(uint16类型) + 数据
	topicBytes := []byte(req.Topic)
	if err := binary.Write(buf, binary.BigEndian, uint16(len(topicBytes))); err != nil {
		return nil, err
	}
	buf.Write(topicBytes)

	// 分区号
	if err := binary.Write(buf, binary.BigEndian, req.Partition); err != nil {
		return nil, err
	}

	// 起始偏移量
	if err := binary.Write(buf, binary.BigEndian, req.StartOffset); err != nil {
		return nil, err
	}

	// 最大消息数
	if err := binary.Write(buf, binary.BigEndian, req.MaxMsgNum); err != nil {
		return nil, err
	}

	// 生成请求头
	header := RequestHeader{
		Length:          uint32(buf.Len()),
		RequestType:     FetchRequestType,
		CompressionType: CompressionTypeNone,
	}

	// 编码请求头
	headerBytes, err := encodeHeader(header)
	if err != nil {
		return nil, err
	}

	// 组装完整请求
	result := append(headerBytes, buf.Bytes()...)
	return result, nil
}

// DecodeFetchRequest 解码拉取请求
func DecodeFetchRequest(data []byte) (*FetchRequest, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(data)
	defer bufferPool.Put(buf)

	req := &FetchRequest{}

	// 解析请求头
	header := RequestHeader{}
	if err := binary.Read(buf, binary.BigEndian, &header.Length); err != nil {
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &header.RequestType); err != nil {
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &header.CompressionType); err != nil {
		return nil, err
	}

	// 读取主题长度
	var topicLen uint16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}

	// 读取主题
	topicBytes := make([]byte, topicLen)
	if _, err := buf.Read(topicBytes); err != nil {
		return nil, err
	}
	req.Topic = string(topicBytes)

	// 读取分区、偏移量和最大消息数
	if err := binary.Read(buf, binary.BigEndian, &req.Partition); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &req.StartOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &req.MaxMsgNum); err != nil {
		return nil, err
	}

	return req, nil
}

// CompressWithSnappy 使用Snappy压缩数据
func CompressWithSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// DecompressWithSnappy 使用Snappy解压数据
func DecompressWithSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

// 缓冲池，用于减少内存分配
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
