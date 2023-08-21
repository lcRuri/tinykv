# Project2 RaftKV

Raft 是一种[共识算法](https://so.csdn.net/so/search?q=共识算法&spm=1001.2101.3001.7020)，其设计理念是易于理解。我们可以在[Raft网站](https://raft.github.io/)上阅读关于 Raft 的材料，Raft 的交互式可视化，以及其他资源，包括[Raft的扩展论文](https://raft.github.io/raft.pdf)。

在这个项目中，将实现一个基于raft的高可用kv服务器，这不仅需要实现 Raft 算法，还需要实际使用它，这会带来更多的挑战，比如用 badger 管理 Raft 的持久化状态，为快照信息添加流控制等。

该项目有3个部分需要去实现，包括：

- **实现基本的 Raft 算法**
- **在 Raft 之上建立一个容错的KV服务**
- **增加对 raftlog GC 和快照的支持**

## Part A

### 代码

在这一部分，将实现基本的 Raft 算法。需要实现的代码在 raft/ 下。在 raft/ 里面，有一些框架代码和测试案例。在这里实现的 Raft 算法有一个与上层应用程序的接口。此外，它使用一个逻辑时钟（这里命名为 tick ）来测量选举和心跳超时，而不是物理时钟。也就是说，不要在Raft模块本身设置一个计时器，上层应用程序负责通过调用 RawNode.Tick() 来推进逻辑时钟。除此之外，消息的发送和接收以及其他事情都是异步处理的，何时真正做这些事情也是由上层应用决定的（更多细节见下文）。例如，Raft 不会在阻塞等待任何请求消息的响应。

在实现之前，请先查看这部分的提示。另外，你应该粗略看一下proto文件 proto/proto/eraftpb.proto 。那里定义了 Raft 发送和接收消息以及相关的结构，你将使用它们来实现。注意，**与 Raft 论文不同，它将心跳和 AppendEntries 分为不同的消息**，以使逻辑更加清晰。

这一部分可以分成3个步骤，包括：

- 领导者选举
- 日志复制
- 原始节点接口

### 实现 Raft 算法

raft/raft.go 中的 raft.Raft 提供了 Raft 算法的核心，包括消息处理、驱动逻辑时钟等。关于更多的实现指南，请查看 raft/doc.go ，其中包含了概要设计和这些MessageTypes 负责的内容。

#### 领导者选举

为了实现领导者选举，你可能想从 raft.Raft.tick() 开始，它被用来通过一个 tick 驱动内部逻辑时钟，从而驱动选举超时或心跳超时。你现在不需要关心消息的发送和接收逻辑。**如果你需要发送消息，只需将其推送到 raft.Raft.msgs** ，Raft 收到的**所有消息将被传递到 raft.Raft.Step()**。**测试代码将从 raft.Raft.msgs 获取消息**，并通过**raft.Raft.Step()** 传递响应消息。raft.Raft.Step() 是消息处理的入口，你应该处理像MsgRequestVote、MsgHeartbeat 这样的消息及其响应。**也请实现 test stub 函数，并让它们被正确调用**，如raft.Raft.becomeXXX，当 Raft 的角色改变时，它被用来更新 Raft 的内部状态。

你可以运行make project2aa来测试实现，并在这部分的最后看到一些提示。

#### 日志复制

为了实现日志复制，你可能想从处理发送方和接收方的 MsgAppend 和MsgAppendResponse 开始。查看 **raft/log.go 中的 raft.RaftLog**，这是一个辅助结构，可以帮助你管理 raft 日志，在这里还需要通过 raft/storage.go 中定义的 Storage 接口与上层应用进行交互，以获得日志项和快照等持久化数据。

你可以运行make project2ab来测试实现，并在这部分的最后看到一些提示。

### 实现原始节点接口

raft/rawnode.go 中的 raft.RawNode 是与上层应用程序交互的接口，raft.RawNode 包含 raft.Raft 并提供一些封装函数，如 RawNode.Tick() 和 RawNode.Step() 。它还提供了 RawNode.Propose() 来让上层应用提出新的 Raft 日志。

另一个重要的结构 **Ready** 也被定义在这里。在处理消息或推进逻辑时钟时，raft.Raft 可能需要与上层应用进行交互，比如：

- 向其他 peer 发送消息
- 将日志项保存到稳定存储中
- 将term、commit index 和 vote 等 hard state 保存到稳定存储中
- 将已提交的日志条目应用于状态机
- 等等

但是这些交互不会立即发生，相反，它们被封装在 Ready 中并由 RawNode.Ready() 返回给上层应用程序。这取决于上层应用程序何时调用 RawNode.Ready() 并处理它。在处理完返回的 Ready 后，上层应用程序还需要调用一些函数，如 RawNode.Advance() 来更新 raft.Raft 的内部状态，如apply\ied index、stabled log index等。

你可以运行make project2ac来测试实现，运行make project2a来测试整个A部分。

> 提示：
>
> - 在 raft.Raft、raft.RaftLog、raft.RawNode 和 eraftpb.proto 上添加任何你需要的状态。
> - 测试假设第一次启动的 Raft 应该有 term 0。
> - 测试假设新当选的领导应该在其任期内附加一个 noop 日志项。
> - 测试没有为本地消息、MessageType_MsgHup、MessageType_MsgBeat和 MessageType_MsgPropose 设置term。
> - 在领导者和非领导者之间，追加的日志项是相当不同的，有不同的来源、检查和处理，要注意这一点。
> - 不要忘了选举超时在 peers 之间应该是不同的。
> - rawnode.go 中的一些封装函数可以用 raft.Step(local message) 实现。
> - 当启动一个新的 Raft 时，从 Storage 中获取最后的稳定状态来初始化raft.Raft 和 raft.RaftLog。

## B部分

在这一部分中，你将使用 Part A 中实现的 Raft 模块建立一个容错的KV存储服务。服务将是一个复制状态机，由几个使用 Raft 进行复制的KV服务器组成。KV服务应该继续处理客户的请求，只要大多数的服务器是活的并且可以通信，尽管有其他的故障或网络分区发生。

在 Project1 中，你已经实现了一个独立的kv服务器，所以你应该已经熟悉了kv服务器的 API 和 Storage 接口。

在介绍代码之前，你需要先了解三个术语：Store、Peer 和 Region，它们定义在proto/proto/metapb.proto 中。

- Store 代表 tinykv-server 的一个实例
- Peer 代表运行在 Store 上的 Raft 节点
- Region 是 Peer 的集合，也叫 Raft 组
  ![在这里插入图片描述](https://img-blog.csdnimg.cn/68f3484bdc9f4fce8ecf52ad43a56ea8.png?x-oss-process=image/watermark,type_ZHJvaWRzYW5zZmFsbGJhY2s,shadow_50,text_Q1NETiBAbXJ4cw==,size_20,color_FFFFFF,t_70,g_se,x_16#pic_center)

为了简单起见，在 Project2 的集群中，一个 Store 上只有一个 Peer，一个 Region。所以现在不需要考虑Region的范围。多个区域将在 Project3 中进一步引入。

### 代码

首先，你应该看看位于 kv/storage/raft_storage/raft_server.go 中的 RaftStorage 的代码，它也实现了存储接口。与 StandaloneStorage 直接从底层引擎写入或读取不同，它首先将每个写入或读取请求发送到 Raft，然后在 Raft 提交请求后对底层引擎进行实际写入和读取。通过这种方式，它可以保持多个 Stores 之间的一致性。

RaftStorage 主要创建一个 Raftstore 来驱动 Raft。当调用 Reader 或 Write 函数时，它实际上是通过 channel （该通道的接收者是 **raftWorker 的 raftCh**）向 raftstore 发送 proto/proto/raft_cmdpb.proto 中定义的 **RaftCmdRequest**，其中有四种基本的命令类型（Get/Put/Delete/Snap），在 Raft 提交并应用该命令后返回响应。而读写函数的 **kvrpc.Context** 参数现在很有用，它从客户端的角度携带了 Region 信息，并作为 **RaftCmdRequest 的 Head** 传递。也许这些**信息是错误的或过时的，所以 raftstore 需要检查**它们并决定是否提出请求。

然后，这里是TinyKV的核心 - **raftstore**。这个结构有点复杂，你可以阅读TiKV的参考资料，让你对这个设计有更好的了解。

https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore (中文版本)
https://pingcap.com/blog/2017-08-15-multi-raft/#raftstore (英文版本)

raftstore 的入口是 Raftstore，见 kv/raftstore/raftstore.go。它启动了一些Worker 来异步处理特定的任务，现在大部分都没有用到，所以你可以直接忽略它们。你所需要关注的是 raftWorker。(kv/raftstore/raft_worker.go)

整个过程分为两部分：raft worker 轮询 raftCh 以获得消息，这些消息包括驱动 Raft 模块的基本 tick 和作为 Raft 日志项的 Raft 命令；它从 Raft 模块获得并处理 ready，包括发送raft消息、持久化状态、将提交的日志项应用到状态机。一旦应用，将响应返回给客户。

### 实现 peer storage

peer storage 是通过 Part A 中的存储接口进行交互，但是除了 raft 日志之外，peer storage 还管理着其他持久化的元数据，这对于重启后恢复到一致的状态机非常重要。此外，在 proto/proto/raft_serverpb.proto 中定义了三个重要状态。

- RaftLocalState：用于存储当前 Raft hard state 和 Last Log Index。
- RaftApplyState。用于存储 Raft applied 的 Last Log Index 和一些 truncated Log 信息。
- RegionLocalState。用于存储 Region 信息和该 Store 上的 Peer State。Normal表示该 peer 是正常的，Tombstone表示该 peer 已从 Region 中移除，不能加入Raft 组。

这些状态被存储在两个badger实例中：raftdb 和 kvdb。

- raftdb 存储 raft 日志和 RaftLocalState。
- kvdb 在不同的列族中存储键值数据，RegionLocalState 和 RaftApplyState。你可以把 kvdb 看作是Raft论文中提到的状态机。

格式如下，在 kv/raftstore/meta 中提供了一些辅助函数，并通writebatch.SetMeta() 将其保存到 badger。

| Key              | KeyFormat                        | Value            | DB   |
| ---------------- | -------------------------------- | ---------------- | ---- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

> 你可能想知道为什么 TinyKV 需要两个 badger 实例。实际上，它只能使用一个badger 来存储 Raft 日志和状态机数据。分成两个实例只是为了与TiKV的设计保持一致。

这些元数据应该在 PeerStorage 中创建和更新。当创建 PeerStorage 时，见kv/raftstore/peer_storage.go 。它初始化这个 Peer 的 RaftLocalState、RaftApplyState，或者在重启的情况下从底层引擎获得之前的值。**注意，RAFT_INIT_LOG_TERM 和 RAFT_INIT_LOG_INDEX 的值都是5（只要大于1），但不是0。**之所以不设置为0，是为了区别于 peer 在更改 conf 后被动创建的情况。你现在可能还不太明白，所以只需记住它，细节将在 project3b 中描述，当你实现 conf change 时。

在这部分你需要实现的代码只有一个函数 PeerStorage.SaveReadyState，这个函数的作用是将 raft.Ready 中的数据保存到 badger 中，包括追加日志和保存 Raft 硬状态。

要追加日志，只需将 raft.Ready.Entries 处的所有日志保存到 raftdb，并删除之前追加的任何日志，这些日志永远不会被提交。同时，更新 peer storage 的RaftLocalState 并将其保存到 raftdb。

保存硬状态也很容易，只要更新peer storage 的 RaftLocalState.HardState 并保存到raftdb。

> 提示:
>
> - 使用WriteBatch来一次性保存这些状态。
> - 关于如何读写这些状态，请参见 peer_storage.go 的其他函数。

### 实现Raft ready 过程

在 Project2 的 PartA ，你已经建立了一个基于 tick 的 Raft 模块。现在你需要编写驱动它的外部流程。大部分代码已经在 kv/raftstore/peer_msg_handler.go 和kv/raftstore/peer.go 下实现。所以你需要学习这些代码，完成proposalRaftCommand 和 HandleRaftReady 的逻辑。下面是对该框架的一些解释。

Raft RawNode 已经用 PeerStorage 创建并存储在 peer 中。在 raft Worker 中，你可以看到它接收了 peer 并通过 peerMsgHandler 将其包装起来。peerMsgHandler主要有两个功能：一个是 HandleMsgs，另一个是 HandleRaftReady。

HandleMsgs 处理所有从 raftCh 收到的消息，包括调用 RawNode.Tick() 驱动Raft的MsgTypeTick、包装来自客户端请求的 MsgTypeRaftCmd 和 Raft peer 之间传送的MsgTypeRaftMessage。所有的消息类型都在 kv/raftstore/message/msg.go 中定义。你可以查看它的细节，其中一些将在下面的部分中使用。

在消息被处理后，Raft 节点应该有一些状态更新。所以 HandleRaftReady 应该从Raft 模块获得Ready，并做相应的动作，如持久化日志，应用已提交的日志，并通过网络向其他 peer 发送 raft 消息。

在一个伪代码中，raftstore 使用 Raft:

```go
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
}


12345678910111213141516
```

在这之后，整个读或写的过程将是这样的：

- 客户端调用 RPC RawGet/RawPut/RawDelete/RawScan
- RPC 处理程序调用 RaftStorage 的相关方法
- RaftStorage 向 raftstore 发送一个 Raft 命令请求，并等待响应
- RaftStore 将 Raft 命令请求作为 Raft Log 提出。
- Raft 模块添加该日志，并由 PeerStorage 持久化。
- Raft 模块提交该日志
- Raft Worker 在处理 Raft Ready 时执行 Raft 命令，并通过 callback 返回响应。
- RaftStorage 接收来自 callback 的响应，并返回给 RPC 处理程序。
- RPC 处理程序进行一些操作并将 RPC 响应返回给客户。

你应该运行 make project2b 来通过所有的测试。整个测试正在运行一个模拟集群，包括多个 TinyKV 实例和一个模拟网络。它执行一些读和写的操作，并检查返回值是否符合预期。

要注意的是，错误处理是通过测试的一个重要部分。你可能已经注意到，在proto/proto/errorpb.proto 中定义了一些错误，错误是 gRPC 响应的一个字段。同时，在 kv/raftstore/util/error.go 中定义了实现 error 接口的相应错误，所以你可以把它们作为函数的返回值。

这些错误主要与 Region 有关。所以它也是 RaftCmdResponse 的 RaftResponseHeader 的一个成员。当提出一个请求或应用一个命令时，可能会出现一些错误。如果是这样，你应该返回带有错误的 Raft 命令响应，然后错误将被进一步传递给 gRPC 响应。你可以使用 kv/raftstore/cmd_resp.go 中提供的 **BindErrResp**，在返回带有错误的响应时，将这些错误转换成 errorpb.proto 中定义的错误。

在这个阶段，你可以考虑这些错误，其他的将在 Project3 中处理。

ErrNotLeader：raft 命令是在一个 Follower 上提出的。所以用它来让客户端尝试其他 peer。
ErrStaleCommand：可能由于领导者的变化，一些日志没有被提交，就被新的领导者的日志所覆盖。但是客户端并不知道，仍然在等待响应。所以你应该返回这个命令，让客户端知道并再次重试该命令。

> 提示：
>
> - PeerStorage 实现了 Raft 模块的存储接口，你应该使用提供的SaveRaftReady() 方法来持久化Raft的相关状态。
> - 使用 engine_util 中的 WriteBatch 来进行原子化的多次写入，例如，你需要确保在一个写入批次中应用提交的日志并更新应用的索引。
> - 使用 Transport 向其他 peer 发送 raft 消息，它在 GlobalContext 中。
> - 服务器不应该完成 RPC，如果它不是多数节点的一部分，并且没有最新的数据。你可以直接把获取操作放到 Raft 日志中，或者实现 Raft 论文第8节中描述的对只读操作的优化。
> - 在应用日志时，不要忘记更新和持久化应用状态机。
> - 你可以像TiKV那样以异步的方式应用已提交的Raft日志条目。这不是必须的，虽然对提高性能是一个很大的提升。
> - 提出命令时记录命令的 callback，应用后返回 callback。
> - 对于 snap 命令的响应，应该明确设置 badger Txn 为 callback。

## C部分

就目前你的代码来看，对于一个长期运行的服务器来说，永远记住完整的Raft日志是不现实的。相反，服务器会检查Raft日志的数量，并不时地丢弃超过阈值的日志。

在这一部分，你将在上述两部分实现的基础上实现快照处理。一般来说，Snapshot 只是一个像 AppendEntries 一样的 Raft 消息，用来复制数据给 Follower，不同的是它的大小，Snapshot 包含了某个时间点的整个状态机数据，一次性建立和发送这么大的消息会消耗很多资源和时间，可能会阻碍其他 Raft 消息的处理，为了避免这个问题，Snapshot 消息会使用独立的连接，把数据分成几块来传输。这就是为什么TinyKV 服务有一个快照 RPC API 的原因。如果你对发送和接收的细节感兴趣，请查看 snapRunner 和参考资料 https://pingcap.com/blog-cn/tikv-source-code-reading-10/

### 代码

你所需要修改的是基于 Part A 和Part B 的代码。

### 在Raft中实现

尽管我们需要对快照信息进行一些不同的处理，但从 Raft 算法的角度来看，应该没有什么区别。请看 proto 文件中 eraftpb.Snapshot 的定义，eraftpb.Snapshot 的数据字段并不代表实际的状态机数据，而是一些元数据，用于上层应用，你可以暂时忽略它。当领导者需要向跟随者发送快照消息时，它可以调用 Storage.Snapshot() 来获取 eraftpb.Snapshot ，然后像其他 raft 消息一样发送快照消息。状态机数据如何实际建立和发送是由 raftstore 实现的，它将在下一步介绍。你可以认为，一旦Storage.Snapshot() 成功返回，Raft 领导者就可以安全地将快照消息发送给跟随者，跟随者应该调用 handleSnapshot 来处理它，即只是从消息中的eraftpb.SnapshotMetadata 恢复 Raft 的内部状态，如term、commit index和成员信息等，之后快照处理的过程就结束了。

### 在raftstore中实现

在这一步，你需要学习 raftstore 的另外两个Worker : raftlog-gc Worker 和 region Worker。

Raftstore 根据配置 RaftLogGcCountLimit 检查它是否需要 gc 日志，见 onRaftGcLogTick()。如果是，它将提出一个 Raft admin 命令 CompactLogRequest，它被封装在 RaftCmdRequest 中，就像 project2 的 Part B 中实现的四种基本命令类型（Get/Put/Delete/Snap）。但与Get/Put/Delete/Snap命令写或读状态机数据不同，CompactLogRequest 是修改元数据，即更新RaftApplyState 中的 RaftTruncatedState。之后，你应该通过ScheduleCompactLog 给 raftlog-gc worker 安排一个任务。Raftlog-gc worker 将以异步方式进行实际的日志删除工作。

然后，由于日志压缩，Raft 模块可能需要发送一个快照。PeerStorage 实现了Storage.Snapshot()。TinyKV 生成快照并在 Region Worker 中应用快照。当调用Snapshot() 时，它实际上是向 Region Worker 发送一个任务 RegionTaskGen。region worker 的消息处理程序位于 kv/raftstore/runner/region_task.go 中。它扫描底层引擎以生成快照，并通过通道发送快照元数据。在下一次 Raft 调用 Snapshot时，它会检查快照生成是否完成。如果是，Raft应该将快照信息发送给其他 peer，而快照的发送和接收工作则由 kv/storage/raft_storage/snap_runner.go 处理。你不需要深入了解这些细节，只需要知道快照信息在收到后将由 onRaftMsg 处理。

然后，快照将反映在下一个Raft ready中，所以你应该做的任务是修改 Raft ready 流程以处理快照的情况。当你确定要应用快照时，你可以更新 peer storage 的内存状态，如 RaftLocalState、RaftApplyState 和 RegionLocalState。另外，不要忘记将这些状态持久化到 kvdb 和 raftdb，并从 kvdb 和 raftdb 中删除陈旧的状态。此外，你还需要将 PeerStorage.snapState 更新为 snap.SnapState_Applying，并通过PeerStorage.regionSched 将 runner.RegionTaskApply 任务发送给 region worker，等待 region worker 完成。

你应该运行make project2c来通过所有的测试。