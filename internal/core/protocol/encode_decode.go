/*
消息编解码

定义二进制协议格式(参考Kafka)
处理压缩(Snappy)
*/
package protocol

import (
	"bytes"
	"encoding/binary"
	"hash/crc32" // simplify waitlist
	"ryanMQ/internal/utils/rlog"
	"sync"
	"time"

	"github.com/golang/snappy"
)

const (
	ProduceRequestType       = byte(0x01)
	FetchRequestType         = byte(0x02)
	OffsetCommitType         = byte(0x03)
	TopicMetadataRequestType = byte(0x04)

	CompressionTypeSnappy = byte(0x01)
)

var crcTable = crc32.MakeTable(crc32.IEEE)

func EncodeProduceRequest(req *ProduceRequest) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer) //写入缓冲区
	defer bufferPool.Put(buf)
	buf.Reset()
	//主题 ：2 字节长度(uint16类型) + 数据
	topicBytes := []byte(req.Topic)
	//大端字节序存储
	if err := binary.Write(buf, binary.BigEndian, uint16(len(topicBytes))); err != nil {
		return nil, err
	}
	buf.Write(topicBytes)

	//分区号：4字节
	binary.Write(buf, binary.BigEndian, req.Partition)

	//消息数量：4字节
	req.msg_cnt = uint32(len(req.Messages))
	binary.Write(buf, binary.BigEndian, req.msg_cnt)

	//消息列表 (每条消息： 4字节 长度(len) + 数据)
	for _, msg := range req.Messages {
		msg.msg_len = uint32(len(msg.data))
		msg.timestamp = uint64(time.Now().Unix())

		binary.Write(buf, binary.BigEndian, msg.msg_len) //Message的长度
		binary.Write(buf, binary.BigEndian, msg.timestamp)
		buf.Write(msg.data)

		bytes := buf.Bytes()
		msg.CRC = crc32.Checksum(bytes[8:], crcTable) //闪开mes_len和crc字段
		binary.Write(buf, binary.BigEndian, msg.CRC)
	}

	//构建最终请求（添加请求头和长度）
	header := RequestHeader{
		Length:          uint32(buf.Len()),
		RequestType:     ProduceRequestType, //生产者请求
		CompressionType: CompressionTypeSnappy,
	}
	headerBytes, err := encodeHeader(header)
	if err != nil {
		rlog.Error("encode header error: %v", err)
		return nil, err
	}

	return append(headerBytes, buf.Bytes()...), nil
}

// 编码请求头
func encodeHeader(header RequestHeader) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	binary.Write(buf, binary.BigEndian, header.Length)
	binary.Write(buf, binary.BigEndian, header.RequestType)
	binary.Write(buf, binary.BigEndian, header.CompressionType)

	return buf.Bytes(), nil
}

// 解码生产请求
func DecodeProduceRequest(data []byte) (*ProduceRequest, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(data)
	defer bufferPool.Put(buf)

	req := &ProduceRequest{}

	//解析请求头
	header := RequestHeader{}
	if err := binary.Read(buf, binary.BigEndian, &header); err != nil {
		rlog.Error("decode header error: %v", err)
		return nil, err
	}
	if header.Length > buf.Len()-6 {
		rlog.Error("header length error")
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &req); err != nil {
		rlog.Error("decode produce request error: %v", err)
		return nil, err
	}

	//req 消息验证
	for _, msg := range req.Messages {
		if ok, err := VerifyMessages(req.Messages); !ok {
			rlog.Error("ProduceRequest Messages verify error: %v", err)
			return nil, err
		}
	}

	return req, nil
}

// 编码生产者响应
func EncodeProduceResponse(resp *ProduceResponse) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	buf.Write(resp.Status)
	binary.Write(buf, binary.BigEndian, resp.Offset)

	return buf.Bytes(), nil
}

// 解码生产者响应
func DecodeProduceResponse(data []byte) (*ProduceResponse, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(data)
	defer bufferPool.Put(buf)

	resp := &ProduceResponse{}
	if err := binary.Read(buf, binary.BigEndian, &resp); err != nil {
		rlog.Error("decode produce Response error: %v", err)
		return nil, err
	}

	return resp, nil
}

// 编码获取消息请求
func EncodeFetchRequest(req *FetchRequest) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	buf.Write([]byte(req.Topic))
	binary.Write(buf, binary.BigEndian, req.Partition)
	binary.Write(buf, binary.BigEndian, req.StartOffset)
	binary.Write(buf, binary.BigEndian, req.MaxMsgNum)

	header := RequestHeader{
		Length:          uint32(buf.Len()),
		RequestType:     FetchRequestType,
		CompressionType: CompressionTypeSnappy,
	}

	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		rlog.Error("write fetch request header error: %v", err)
		return nil, err
	}

	return buf.Bytes(), nil
}

// 解码获取消息请求
func DecodeFetchRequest(data []byte) (*FetchRequest, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(data)
	defer bufferPool.Put(buf)

	req := &FetchRequest{}
	if err := binary.Read(buf, binary.BigEndian, &req); err != nil {
		rlog.Error("decode fetch request error: %v", err)
		return nil, err
	}

	return req, nil
}

// 编码获取消息响应
func EncodeFetchResponse(resp *FetchResponse) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	buf.Write(resp.Status)
	binary.Write(buf, binary.BigEndian, resp.Messages)

	return buf.Bytes(), nil
}

// 解码获取消息响应
func DecodeFetchResponse(data []byte) (*FetchResponse, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(data)
	defer bufferPool.Put(buf)

	resp := &FetchResponse{}
	if err := binary.Read(buf, binary.BigEndian, &resp); err != nil {
		rlog.Error("decode fetch response error: %v", err)
		return nil, err
	}

	//resp 消息验证
	for _, msg := range resp.Messages {
		if ok, err := VerifyMessages([]Message{msg}); !ok {
			rlog.Error("FetchResponse Messages verify error: %v", err)
			return nil, err
		}
	}

	return resp, nil
}

// utils
func CompressWithSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func DecompressWithSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func VerifyMessages(msgs []Message) (bool, error) {
	for _, msg := range msgs {
		crc := crc32.Checksum(msg.data[8:], crcTable)
		if msg.CRC != crc {
			return false, nil
		}
	}
	return true, nil
}
