package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	// IndexEntrySize 索引项大小（8字节偏移量 + 8字节物理位置）
	IndexEntrySize = 16
)

// Index 表示消息索引，实现稀疏索引（跳表结构）
// 将逻辑偏移量映射到段文件中的物理位置
type Index struct {
	file            *os.File
	position        uint64
	baseOffset      uint64
	entries         []byte
	entriesCount    uint64
	indexInterval   uint64 // 每隔多少条消息创建一个索引
	writeLock       sync.RWMutex
	lastIndexedPos  uint64
	lastIndexOffset uint64
}

// IndexOption 索引配置选项
type IndexOption func(*Index)

// WithIndexInterval 设置索引间隔
func WithIndexInterval(interval uint64) IndexOption {
	return func(i *Index) {
		i.indexInterval = interval
	}
}

// NewIndex 创建新的索引
func NewIndex(path string, baseOffset uint64, options ...IndexOption) (*Index, error) {
	index := &Index{
		baseOffset:    baseOffset,
		indexInterval: 100, // 默认每100条消息创建一个索引项
	}

	// 应用选项
	for _, option := range options {
		option(index)
	}

	// 创建目录（如果不存在）
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// 打开或创建索引文件
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	index.file = file

	// 获取文件信息
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat index file: %w", err)
	}

	// 文件大小必须是索引项大小的整数倍
	fileSize := uint64(fi.Size())
	if fileSize%IndexEntrySize != 0 {
		file.Close()
		return nil, fmt.Errorf("corrupt index file: size %d is not a multiple of %d", fileSize, IndexEntrySize)
	}

	// 计算索引项数量
	entriesCount := fileSize / IndexEntrySize
	index.entriesCount = entriesCount
	index.position = fileSize

	// 如果有索引项，读取最后一个项的偏移量
	if entriesCount > 0 {
		buffer := make([]byte, IndexEntrySize)
		if _, err := file.ReadAt(buffer, int64(fileSize-IndexEntrySize)); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read last index entry: %w", err)
		}
		index.lastIndexOffset = binary.BigEndian.Uint64(buffer[:8])
		index.lastIndexedPos = binary.BigEndian.Uint64(buffer[8:])
	}

	// 将整个文件读入内存
	entries := make([]byte, fileSize)
	if fileSize > 0 {
		if _, err := file.ReadAt(entries, 0); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read index entries: %w", err)
		}
	}
	index.entries = entries

	return index, nil
}

// AddEntry 添加索引项
func (i *Index) AddEntry(offset, position uint64) error {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	// 检查是否需要索引
	relOffset := offset - i.baseOffset
	if relOffset-i.lastIndexOffset < i.indexInterval && i.entriesCount > 0 {
		return nil // 不满足索引间隔要求，跳过
	}

	// 准备索引项数据
	entry := make([]byte, IndexEntrySize)
	binary.BigEndian.PutUint64(entry[:8], relOffset)
	binary.BigEndian.PutUint64(entry[8:], position)

	// 追加到内存中的索引
	newEntries := make([]byte, i.position+IndexEntrySize)
	copy(newEntries, i.entries)
	copy(newEntries[i.position:], entry)
	i.entries = newEntries

	// 写入到文件
	if _, err := i.file.WriteAt(entry, int64(i.position)); err != nil {
		return fmt.Errorf("failed to write index entry: %w", err)
	}

	// 更新状态
	i.position += IndexEntrySize
	i.entriesCount++
	i.lastIndexOffset = relOffset
	i.lastIndexedPos = position

	return nil
}

// Lookup 查找离给定偏移量最近但不大于偏移量的索引项
func (i *Index) Lookup(offset uint64) (uint64, error) {
	i.writeLock.RLock()
	defer i.writeLock.RUnlock()

	// 如果没有索引项，返回错误
	if i.entriesCount == 0 {
		return 0, fmt.Errorf("index is empty")
	}

	// 如果要查找的偏移量小于基础偏移量，返回错误
	if offset < i.baseOffset {
		return 0, fmt.Errorf("offset %d is out of range (minimum: %d)", offset, i.baseOffset)
	}

	// 计算相对偏移量
	relOffset := offset - i.baseOffset

	// 二分查找
	var low, high, mid uint64 = 0, i.entriesCount - 1, 0
	for low <= high {
		mid = (low + high) / 2
		midOffset := binary.BigEndian.Uint64(i.entries[mid*IndexEntrySize : mid*IndexEntrySize+8])

		if midOffset == relOffset {
			// 找到精确匹配
			return binary.BigEndian.Uint64(i.entries[mid*IndexEntrySize+8 : (mid+1)*IndexEntrySize]), nil
		} else if midOffset < relOffset {
			if mid == i.entriesCount-1 || binary.BigEndian.Uint64(i.entries[(mid+1)*IndexEntrySize:(mid+1)*IndexEntrySize+8]) > relOffset {
				// 找到最接近但不大于的项
				return binary.BigEndian.Uint64(i.entries[mid*IndexEntrySize+8 : (mid+1)*IndexEntrySize]), nil
			}
			low = mid + 1
		} else {
			if mid == 0 {
				// 所有索引都大于请求的偏移量
				return 0, fmt.Errorf("offset %d is out of range (minimum indexed: %d)",
					offset, binary.BigEndian.Uint64(i.entries[:8])+i.baseOffset)
			}
			high = mid - 1
		}
	}

	// 返回找到的位置
	return binary.BigEndian.Uint64(i.entries[high*IndexEntrySize+8 : (high+1)*IndexEntrySize]), nil
}

// Close 关闭索引文件
func (i *Index) Close() error {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	if i.file != nil {
		if err := i.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync index file: %w", err)
		}
		if err := i.file.Close(); err != nil {
			return fmt.Errorf("failed to close index file: %w", err)
		}
		i.file = nil
	}
	return nil
}

// Remove 删除索引文件
func (i *Index) Remove() error {
	i.writeLock.Lock()
	defer i.writeLock.Unlock()

	if i.file != nil {
		path := i.file.Name()
		if err := i.file.Close(); err != nil {
			return fmt.Errorf("failed to close index file: %w", err)
		}
		i.file = nil

		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove index file: %w", err)
		}
	}
	return nil
}

// LastOffset 返回最后一个索引项的偏移量
func (i *Index) LastOffset() uint64 {
	i.writeLock.RLock()
	defer i.writeLock.RUnlock()

	if i.entriesCount == 0 {
		return i.baseOffset
	}
	return i.lastIndexOffset + i.baseOffset
}

// Size 返回索引当前使用的字节数
func (i *Index) Size() uint64 {
	i.writeLock.RLock()
	defer i.writeLock.RUnlock()
	return i.position
}

// EntryCount 返回索引项数量
func (i *Index) EntryCount() uint64 {
	i.writeLock.RLock()
	defer i.writeLock.RUnlock()
	return i.entriesCount
}
