package storage

// 索引接口，基于稀疏索引实现
type Index interface {
	// 添加索引项
	AddEntry(offset uint64, position uint32) error

	// 查找最接近但不超过指定offset的物理位置
	Lookup(offset uint64) (uint32, error)

	// 关闭索引
	Close() error
}
