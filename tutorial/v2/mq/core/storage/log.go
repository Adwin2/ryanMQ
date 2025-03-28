package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrOffsetOutOfRange = errors.New("offset is out of range")
	ErrLogClosed        = errors.New("log is closed")
)

// Log 管理多个日志段的集合
type Log struct {
	dir            string
	segments       []*Segment
	activeSegment  *Segment
	mu             sync.RWMutex
	baseOffset     uint64
	nextOffset     uint64
	segmentMaxSize uint64
	indexInterval  uint64
	indexes        map[uint64]*Index
	closed         bool
}

// LogConfig 日志配置
type LogConfig struct {
	Dir            string // 存储目录
	SegmentMaxSize uint64 // 段文件最大大小
	IndexInterval  uint64 // 索引间隔
}

// NewLog 创建新的日志管理器
func NewLog(config LogConfig) (*Log, error) {
	if config.Dir == "" {
		return nil, errors.New("directory cannot be empty")
	}

	if config.SegmentMaxSize == 0 {
		config.SegmentMaxSize = 1024 * 1024 * 1024 // 默认1GB
	}

	if config.IndexInterval == 0 {
		config.IndexInterval = 100 // 默认每100条消息创建索引
	}

	// 创建目录（如果不存在）
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	log := &Log{
		dir:            config.Dir,
		segments:       make([]*Segment, 0),
		segmentMaxSize: config.SegmentMaxSize,
		indexInterval:  config.IndexInterval,
		indexes:        make(map[uint64]*Index),
		nextOffset:     0,
	}

	// 加载现有段
	if err := log.loadSegments(); err != nil {
		return nil, err
	}

	// 如果没有段，创建初始段
	if len(log.segments) == 0 {
		if err := log.createNewSegment(0); err != nil {
			return nil, err
		}
	}

	return log, nil
}

// 加载现有段文件
func (l *Log) loadSegments() error {
	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// 查找所有段文件
	var baseOffsets []uint64
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// 只处理.log文件
		if !strings.HasSuffix(file.Name(), ".log") {
			continue
		}

		offsetStr := strings.TrimSuffix(file.Name(), ".log")
		baseOffset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			continue // 跳过无法识别的文件
		}

		baseOffsets = append(baseOffsets, baseOffset)
	}

	// 按偏移量排序
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// 加载段文件
	for _, baseOffset := range baseOffsets {
		segment, err := l.loadSegment(baseOffset)
		if err != nil {
			return fmt.Errorf("failed to load segment %d: %w", baseOffset, err)
		}
		l.segments = append(l.segments, segment)
	}

	// 设置活动段和下一个偏移量
	if len(l.segments) > 0 {
		l.activeSegment = l.segments[len(l.segments)-1]
		// 计算下一个消息的偏移量
		l.nextOffset = l.activeSegment.BaseOffset() + l.activeSegment.Size()
		l.baseOffset = l.segments[0].BaseOffset()
	}

	return nil
}

// 加载单个段文件
func (l *Log) loadSegment(baseOffset uint64) (*Segment, error) {
	segmentPath := filepath.Join(l.dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(l.dir, fmt.Sprintf("%020d.index", baseOffset))

	// 加载段
	segment, err := NewSegment(segmentPath, baseOffset, WithMaxBytes(l.segmentMaxSize))
	if err != nil {
		return nil, fmt.Errorf("failed to load segment: %w", err)
	}

	// 加载或创建索引
	index, err := NewIndex(indexPath, baseOffset, WithIndexInterval(l.indexInterval))
	if err != nil {
		segment.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	l.indexes[baseOffset] = index
	return segment, nil
}

// 创建新段
func (l *Log) createNewSegment(baseOffset uint64) error {
	segmentPath := filepath.Join(l.dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(l.dir, fmt.Sprintf("%020d.index", baseOffset))

	// 创建新段
	segment, err := NewSegment(segmentPath, baseOffset, WithMaxBytes(l.segmentMaxSize))
	if err != nil {
		return fmt.Errorf("failed to create segment: %w", err)
	}

	// 创建索引
	index, err := NewIndex(indexPath, baseOffset, WithIndexInterval(l.indexInterval))
	if err != nil {
		segment.Close()
		return fmt.Errorf("failed to create index: %w", err)
	}

	l.segments = append(l.segments, segment)
	l.activeSegment = segment
	l.indexes[baseOffset] = index

	if len(l.segments) == 1 {
		l.baseOffset = baseOffset
	}

	return nil
}

// Append 追加消息到日志
func (l *Log) Append(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrLogClosed
	}

	// 检查活动段是否已满
	if l.activeSegment.IsFull() {
		if err := l.createNewSegment(l.nextOffset); err != nil {
			return 0, fmt.Errorf("failed to create new segment: %w", err)
		}
	}

	// 追加数据到活动段
	position := l.activeSegment.Size()
	offset, err := l.activeSegment.Append(data)
	if err != nil {
		return 0, fmt.Errorf("failed to append to segment: %w", err)
	}

	// 更新索引
	index := l.indexes[l.activeSegment.BaseOffset()]
	if err := index.AddEntry(offset, position); err != nil {
		return 0, fmt.Errorf("failed to add index entry: %w", err)
	}

	// 更新下一个偏移量
	l.nextOffset = offset + 1

	return offset, nil
}

// Read 从指定偏移量读取消息
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogClosed
	}

	// 找到包含偏移量的段
	segment, index, err := l.findSegmentAndIndex(offset)
	if err != nil {
		return nil, err
	}

	// 查找物理位置
	position, err := index.Lookup(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup index: %w", err)
	}

	// 读取数据
	data, err := segment.Read(position)
	if err != nil {
		return nil, fmt.Errorf("failed to read from segment: %w", err)
	}

	return data, nil
}

// findSegmentAndIndex 查找包含指定偏移量的段和索引
func (l *Log) findSegmentAndIndex(offset uint64) (*Segment, *Index, error) {
	// 检查范围
	if offset < l.baseOffset || offset >= l.nextOffset {
		return nil, nil, ErrOffsetOutOfRange
	}

	// 二分查找合适的段
	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].BaseOffset() > offset
	})

	// 如果找到的是第一个段，表示所有段的基础偏移量都大于请求的偏移量
	if idx == 0 {
		return nil, nil, ErrOffsetOutOfRange
	}

	// 使用前一个段（其基础偏移量 <= offset）
	segmentIdx := idx - 1
	segment := l.segments[segmentIdx]
	index := l.indexes[segment.BaseOffset()]

	return segment, index, nil
}

// Close 关闭日志
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	// 关闭所有段
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %w", err)
		}
	}

	// 关闭所有索引
	for _, index := range l.indexes {
		if err := index.Close(); err != nil {
			return fmt.Errorf("failed to close index: %w", err)
		}
	}

	l.closed = true
	return nil
}

// Truncate 删除旧于指定偏移量的所有段
func (l *Log) Truncate(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogClosed
	}

	// 找到第一个基础偏移量大于指定偏移量的段的索引
	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].BaseOffset() > offset
	})

	// 保留的段，需要保留至少一个段
	if idx <= 1 {
		return nil // 不需要截断，或者只有一个段
	}

	// 删除不需要的段
	for i := 0; i < idx-1; i++ {
		segment := l.segments[0]
		baseOffset := segment.BaseOffset()

		// 删除段
		if err := segment.Remove(); err != nil {
			return fmt.Errorf("failed to remove segment: %w", err)
		}

		// 删除索引
		if idx, ok := l.indexes[baseOffset]; ok {
			if err := idx.Remove(); err != nil {
				return fmt.Errorf("failed to remove index: %w", err)
			}
			delete(l.indexes, baseOffset)
		}

		// 从切片中移除
		l.segments = l.segments[1:]
	}

	// 更新基础偏移量
	l.baseOffset = l.segments[0].BaseOffset()

	return nil
}

// GetBaseOffset 获取基础偏移量
func (l *Log) GetBaseOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseOffset
}

// GetNextOffset 获取下一个可用的偏移量
func (l *Log) GetNextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}
