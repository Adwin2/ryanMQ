/*
 消息路由
 根据消息Key哈希到指定分区 （实现负载均衡）

 ​关键技术：
 用 sync.Map 缓存主题元数据
 分区选择算法：hash(key) % partition_num
*/