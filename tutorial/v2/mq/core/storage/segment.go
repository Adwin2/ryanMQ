package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Segment 表示一个日志分段文件
type Segment struct {
	baseOffset     uint64        // 本段的起始偏移量
	path           string        // 文件路径
	maxBytes       uint64        // 段文件最大字节数
	file           *os.File      // 数据文件
	size           uint64        // 当前大小
	mmap           []byte        // 内存映射区域
	position       uint64        // 写入位置
	writeLock      sync.Mutex    // 写入锁
	isClosed       bool          // 是否已关闭
	flushThreshold uint64        // 刷新阈值
	lastFlushPos   uint64        // 最后刷新位置
}

// SegmentOption 段配置选项
type SegmentOption func(*Segment)

// WithMaxBytes 设置段最大字节数
func WithMaxBytes(maxBytes uint64) SegmentOption {
	return func(s *Segment) {
		s.maxBytes = maxBytes
	}
}

// WithFlushThreshold 设置刷新阈值
func WithFlushThreshold(threshold uint64) SegmentOption {
	return func(s *Segment) {
		s.flushThreshold = threshold
	}
}

// NewSegment 创建一个新的日志段
func NewSegment(path string, baseOffset uint64, options ...SegmentOption) (*Segment, error) {
	segment := &Segment{
		baseOffset:     baseOffset,
		path:           path,
		maxBytes:       1024 * 1024 * 1024, // 默认1GB
		flushThreshold: 4096,               // 默认4KB
	}

	// 应用选项
	for _, option := range options {
		option(segment)
	}

	// 创建目录（如果不存在）
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// 打开或创建文件
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}
	segment.file = file

	// 获取文件信息
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment file: %w", err)
	}
	segment.size = uint64(fi.Size())
	segment.position = segment.size

	// 内存映射文件
	if err := segment.mmap(); err != nil {
		file.Close()
		return nil, err
	}

	return segment, nil
}

// mmap 内存映射文件
func (s *Segment) mmap() error {
	// 如果文件为空，先写入一点数据，否则mmap会失败
	if s.size == 0 {
		if err := s.file.Truncate(int64(s.maxBytes)); err != nil {
			return fmt.Errorf("failed to truncate file: %w", err)
		}
		s.size = s.maxBytes
	}

	// 内存映射
	mmap, err := syscall.Mmap(
		int(s.file.Fd()),
		0,
		int(s.size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("failed to mmap segment file: %w", err)
	}
	s.mmap = mmap
	return nil
}

// Append 向段追加数据
func (s *Segment) Append(data []byte) (uint64, error) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if s.isClosed {
		return 0, fmt.Errorf("segment is closed")
	}

	dataLen := uint64(len(data))
	// 检查是否有足够空间存储 (8字节长度 + 数据)
	recordSize := 8 + dataLen
	if s.position+recordSize > s.size {
		return 0, fmt.Errorf("segment is full")
	}

	// 写入数据长度（8字节）
	binary.BigEndian.PutUint64(s.mmap[s.position:s.position+8], dataLen)
	// 写入数据
	copy(s.mmap[s.position+8:s.position+8+dataLen], data)

	// 计算消息的偏移量
	offset := s.baseOffset + s.position

	// 更新写入位置
	s.position += recordSize

	// 如果达到刷新阈值，则刷新到磁盘
	if s.position-s.lastFlushPos >= s.flushThreshold {
		if err := s.Flush(); err != nil {
			return 0, fmt.Errorf("failed to flush segment: %w", err)
		}
	}

	return offset, nil
}

// Read 从指定位置读取数据
func (s *Segment) Read(position uint64) ([]byte, error) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if s.isClosed {
		return nil, fmt.Errorf("segment is closed")
	}

	if position >= s.position {
		return nil, fmt.Errorf("position out of bounds")
	}

	// 读取数据长度
	dataLen := binary.BigEndian.Uint64(s.mmap[position : position+8])
	if position+8+dataLen > s.position {
		return nil, fmt.Errorf("corrupt data: size exceeds available data")
	}

	// 复制数据
	data := make([]byte, dataLen)
	copy(data, s.mmap[position+8:position+8+dataLen])

	return data, nil
}

// Flush 强制将内存中的数据写入磁盘
func (s *Segment) Flush() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if s.isClosed {
		return fmt.Errorf("segment is closed")
	}

	if err := syscall.Msync(s.mmap, syscall.MS_SYNC); err != nil {
		return fmt.Errorf("failed to msync segment file: %w", err)
	}

	s.lastFlushPos = s.position
	return nil
}

// Close 关闭段文件
func (s *Segment) Close() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if s.isClosed {
		return nil
	}

	// 刷新数据
	if err := syscall.Msync(s.mmap, syscall.MS_SYNC); err != nil {
		return fmt.Errorf("failed to msync segment file: %w", err)
	}

	// 解除内存映射
	if err := syscall.Munmap(s.mmap); err != nil {
		return fmt.Errorf("failed to munmap segment file: %w", err)
	}

	// 关闭文件
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close segment file: %w", err)
	}

	s.isClosed = true
	return nil
}

// IsFull 检查段是否已满
func (s *Segment) IsFull() bool {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	// 如果剩余空间不足以存储至少一条最小消息（8字节长度 + 1字节数据），则认为已满
	return s.position+9 >= s.size
}

// Size 返回段当前使用的字节数
func (s *Segment) Size() uint64 {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	return s.position
}

// BaseOffset 返回段的基础偏移量
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

// Remove 删除段文件
func (s *Segment) Remove() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if !s.isClosed {
		if err := s.Close(); err != nil {
			return fmt.Errorf("failed to close segment before removal: %w", err)
		}
	}

	if err := os.Remove(s.path); err != nil {
		return fmt.Errorf("failed to remove segment file: %w", err)
	}

	return nil
}
