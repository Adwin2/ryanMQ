/*
 连接管理

 封包/拆包 （处理粘包）
 心跳维护 （保活机制）


 基于 net.Listener 实现非阻塞 IO
 使用 goroutine-per-connection 模型（Go 高并发优势）
*/