/*
 管理单个分段文件 (.log + .index)
 实现消息追加 func Append(msg []byte)
 构建稀疏索引 4KB/条
 处理文件滚动 达到 segment.max.size 后创建新文件
*/
