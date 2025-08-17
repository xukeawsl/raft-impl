# raft-impl

基于 brpc 和 rocksdb 实现 raft 共识算法，主要是专注于功能实现，性能并不佳，工程上推荐使用 braft，实现如下功能
* 领导者选举
* 日志复制
* 快照安装（不支持分块）

## 依赖

* brpc-1.10.0（protobuf-3.6.1）

* rocksdb-10.4.2

* spdlog-1.15.3

## 测试

* 修改 `gflags.conf` 配置即可，目前配的三个节点，运行时带上节点 ID 即可

* 计数服务的监听端口端口可以运行时通过 `service_port` 指定

```bash
./RaftImpl --node_id=1 --service_port=9054
#...
./RaftImpl --node_id=3 --service_port=9056
```

* 运行客户端程序，需要指定 Leader 节点的地址，目前还不支持重定向请求

```bash
./counter_client --server_addr=127.0.0.1:9054
```

## 参考

* [raft论文中文翻译](https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md)