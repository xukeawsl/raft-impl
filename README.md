# raft-impl

基于 brpc 和 rocksdb 实现 raft 共识算法，目前只实现了 Leader 选举和日志复制，后续会实现快照

## 依赖

* brpc-1.10.0（protobuf-3.6.1）

* rocksdb-10.4.2

* spdlog-1.15.3

## 测试

* 修改 `gflags.conf` 配置即可，目前配的三个节点，运行时带上节点 ID 即可

```bash
./RaftImpl --node_id=1
#...
./RaftImpl --node_id=3
```