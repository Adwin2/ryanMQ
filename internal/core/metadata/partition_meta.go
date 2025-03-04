/*
 分区元信息

 记录Leader/Follower 节点 （分布式需求）
 维护ISR列表(In-Sync Replica)

 ​关键技术：
 用嵌入式数据库（如 BoltDB）存储元数据
*/