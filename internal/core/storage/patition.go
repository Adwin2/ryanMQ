/*
 维护分区下的多个segment
 根据时间/空间策略清理旧数据 retention策略

 ​关键技术：
 使用 os.File + bufio.Writer 实现缓冲写入
 ​零拷贝读取：syscall.Sendfile 直接发送文件内容
 ​内存映射：syscall.Mmap 加速索引文件读取

*/
