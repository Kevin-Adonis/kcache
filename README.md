# kcache


### 功能
<input type="checkbox" checked> grpc通信
<input type="checkbox" checked> etcd注册节点，自动发现节点上下线，并重置哈希函数
<input type="checkbox"> 节点上下线替换更好的pick算法
<input type="checkbox" checked> 热备缓存
<input type="checkbox"> 删除和修改接口
<input type="checkbox"> ttl过期
<input type="checkbox"> 加强高并发(细化锁粒度？)
<input type="checkbox"> 内存池
<input type="checkbox"> 替换LRU算法（算法模块可选）
<input type="checkbox"> 优化项目结构

### 运行方法
```
go run . {any port number}
```