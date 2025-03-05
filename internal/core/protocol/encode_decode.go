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
		msg.msg_len = uint32(len(msg.data))

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

func encodeHeader(header RequestHeader) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	binary.Write(buf, binary.BigEndian, header.Length)
	binary.Write(buf, binary.BigEndian, header.RequestType)
	binary.Write(buf, binary.BigEndian, header.CompressionType)

	return buf.Bytes(), nil
}

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

	return req, nil
}

func EncodeProduceResponse(resp *ProduceResponse) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	buf.Write(resp.Status)
	binary.Write(buf, binary.BigEndian, resp.Offset)

	return buf.Bytes(), nil
}

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

func EncodeFetchRequest(req *FetchRequest) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	binary.Write(buf, binary.BigEndian, req)
	defer bufferPool.Put(buf)
	return buf.Bytes(), nil
}

func DecodeFetchRequest(data []byte) (*FetchRequest, error) {

}

func EncodeFetchResponse() ([]byte, error) {

}

func DecodeFetchResponse(data []byte) (*FetchResponse, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
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
