# 提议转移leader

这一步相对简单，作为raft命令，TransferLeader将被提议为一个raft的日志。但是TransferLeader实际上是一个不需要复制给其他peers的一个行为，所以你只需要调用Rawnode的TransferLeader()方法作为TransferLeader的命令。

# 在raftstore里面实现配置变更

配置变更有两种，增加节点和删除节点。正如名字所说，它从Region中添加一个peer或者删除一个peer。为了实现配置变更，你应该学习首先学习 RegionEpoch 的术语。RegionEpoch 是 metapb 元信息的一部分。当Region添加或删除或拆分peer时，该Region’s epoch 已更改。RegionEpoch 的conf_ver在 ConfChange 期间增加。它将用于保证一个区域中两个领导者在网络隔离下的最新区域信息。

您需要使 raftstore 支持处理 conf change 命令。该过程将是：

1.Propose conf change admin command by `ProposeConfChange`

2.After the log is committed, change the `RegionLocalState`, including `RegionEpoch` and `Peers` in `Region`

3.Call `ApplyConfChange()` of `raft.RawNode`

提示：

1.为了执行 AddNode，新添加的 Peer 将通过 leader 的心跳创建，使用storeWorker 的 maybeCreatePeer()方法。此时，这个 Peer 是未初始化的，我们不知道其 Region 的任何信息，因此我们使用 0 来初始化其 Log Term 和 Index。然后，领导者将知道这个追随者没有数据（存在从 0 到 5 的日志间隙），它会直接向这个追随者发送快照。

2.为了执行 RemoveNode，你应该显式调用 destroyPeer()来停止 Raft 模块。为您提供了销毁逻辑。

3.不要忘记在 GlobalContext 的 storeMeta 中更新区域状态

4.测试代码会多次调度一个 conf 更改的命令，直到应用 conf 更改，因此您需要考虑如何忽略同一 conf 更改的重复命令。

# 实现raftstore分割region

![keyspace](/Users/yefeixiang/coding/tinykv/doc/imgs/keyspace.png)

为了支持多 Raft，系统会进行数据分片，让每个 Raft 组只存储一部分数据。Hash 和 Range 通常用于数据分片。TinyKV 使用 Range，主要原因是 Range 可以更好地聚合具有相同前缀的密钥，方便扫描等操作。此外，Range 在拆分方面的表现优于 Hash。通常，它只涉及元数据修改，不需要移动数据。

```protobuf
message Region {
 uint64 id = 1;
 // Region key range [start_key, end_key).
 bytes start_key = 2;
 bytes end_key = 3;
 RegionEpoch region_epoch = 4;
 repeated Peer peers = 5
}
```

让我们重新看一下区域定义，它包括两个字段 start_key 和 end_key，用于指示区域负责的数据范围。所以 split 是支持多 raft 的关键步骤。一开始，只有一个区域的范围为 [“”， “”）。您可以将键空间视为一个循环，因此 [“”， “”） 代表整个空间。写入数据后，拆分检查器将检查每个 cfg 的区域大小。SplitRegionCheckTickInterval，并生成一个拆分键，如果可能的话，将 Region 切成两部分，可以在 kv/raftstore/runner/split_check.go 中查看逻辑。拆分键将包装为 onPrepareSplitRegion（） 处理的 MsgSplitRegion。

为确保新创建的 Region 和 Peer 的 ID 是唯一的，这些 ID 由调度程序分配。它还提供，因此您不必实现它。onPrepareSplitRegion（） 实际上为 pd worker 调度了一个任务，以向调度器请求 ids。并在收到调度器的响应后发出拆分管理命令，详见 kv/raftstore/runner/scheduler_task.go 中的 onAskSplit（）。

因此，您的任务是实现处理拆分管理命令的过程，就像 conf change 一样。提供的框架支持多个 raft，参见 kv/raftstore/router.go。当一个区域拆分为两个区域时，其中一个区域将继承拆分前的元数据，并仅修改其 Range 和 RegionEpoch，而另一个区域将创建相关的元信息。

提示：

1.这个新创建的 Region 的对应 Peer 应该由 createPeer（） 创建并注册到 router.regions。区域信息应插入到 ctx 的 regionRanges 中

2.对于使用网络隔离拆分的情况区域，要应用的快照可能与现有区域的范围重叠。检查逻辑位于 kv/raftstore/peer_msg_handler.go 的 checkSnapshot（） 中。请在实施时牢记并处理这种情况。

3.使用engine_util。ExceedEndKey（） 与区域的结束键进行比较。因为当结束键等于“”时，任何键都会等于或大于“”。

4.需要考虑更多错误：ErrRegionNotFound、ErrKeyNotInRegion、ErrEpochNotMatch。