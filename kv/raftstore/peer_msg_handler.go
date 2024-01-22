package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}

	ready := d.RaftGroup.Ready()
	result, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		return
	}

	//存储结果所在的region
	if result != nil {
		d.peerStorage.SetRegion(result.Region)
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regions[d.Region().Id] = d.Region()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		d.ctx.storeMeta.Unlock()
	}

	if len(ready.Messages) != 0 {
		d.Send(d.ctx.trans, ready.Messages)
	}

	if len(ready.CommittedEntries) > 0 {
		//log.Infof("process:%d", d.RaftGroup.Raft.Id())
		for _, entry := range ready.CommittedEntries {
			kvWB := new(engine_util.WriteBatch)

			//当日志已经被提交到raft进行记录之后，对日志实际进行处理
			if entry.EntryType == eraftpb.EntryType_EntryConfChange {
				d.processConfChange(&entry, kvWB)
			} else {
				d.process(&entry, kvWB)
			}

			d.peerStorage.applyState.AppliedIndex = entry.Index
			err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}

			//可能会在processConfChange的时候将本身destroy掉了
			if d.stopped {
				kv := new(engine_util.WriteBatch)
				kv.DeleteMeta(meta.ApplyStateKey(d.regionId))
				err = kv.WriteToDB(d.peerStorage.Engines.Kv)
				if err != nil {
					panic(err)
				}
				return
			}

			err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			if err != nil {
				panic(err)
			}

		}
	}
	d.RaftGroup.Advance(ready)

}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		//lab2b
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		log.Infof("%s on old version %d, old config version %d", d.Tag, d.Region().RegionEpoch.Version, d.Region().RegionEpoch.ConfVer)
		log.Infof("%s on split version %d, config version %d", d.Tag, split.RegionEpoch.Version, split.RegionEpoch.ConfVer)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		//log.Infof("err:%s", err.Error())
		cb.Done(ErrResp(err))
		return
	}

	if msg.AdminRequest != nil {
		req := msg.AdminRequest
		switch req.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			_ = d.RaftGroup.Propose(data)
		case raft_cmdpb.AdminCmdType_TransferLeader:
			id := msg.AdminRequest.TransferLeader.Peer.Id
			d.RaftGroup.TransferLeader(id)
			cb.Done(&raft_cmdpb.RaftCmdResponse{
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
				},
				Header: &raft_cmdpb.RaftResponseHeader{
					Error:       nil,
					Uuid:        nil,
					CurrentTerm: d.Term(),
				}})
		case raft_cmdpb.AdminCmdType_ChangePeer:
			ctx, err := msg.Marshal()
			if err != nil {
				return
			}
			confChange := pb.ConfChange{
				ChangeType: req.ChangePeer.ChangeType,
				NodeId:     req.ChangePeer.Peer.Id,
				Context:    ctx,
			}
			p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}

			err = d.RaftGroup.ProposeConfChange(confChange)
			if err != nil {
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				return
			}

			d.proposals = append(d.proposals, p)
		case raft_cmdpb.AdminCmdType_Split:
			err = util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}

			p := &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			}
			_ = d.RaftGroup.Propose(data)

			d.proposals = append(d.proposals, p)
		}
	}

	if len(msg.Requests) > 0 {
		req := msg.Requests[0]
		var key []byte
		//根据msg类型不同进行处理
		//log.Infof("req.CmdType:%v", req.CmdType)
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = req.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		case raft_cmdpb.CmdType_Snap:
		}
		err = util.CheckKeyInRegion(key, d.Region())
		if err != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
			cb.Done(ErrResp(err))
			//msg.Requests = msg.Requests[1:]
			return
		}
		data, err1 := msg.Marshal()
		if err1 != nil && data != nil {
			panic(err)
		}
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)

		err = d.RaftGroup.Propose(data)
		if err != nil {
			panic(err)
		}
	}

}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d", d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
// / 检查消息是否发送到正确的对等方。
// // /
// / 返回 true 表示可以静默删除消息。
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	// 让我们考虑以下三个节点 [1， 2， 3] 且 1 是领导者的情况：
	//	a. 1 删除 2,2 仍可能向 1 发送 MsgAppendResponse。
	//	我们应该忽略这个陈旧的消息，让 2 在之后删除自己
	//	应用 ConfChange 日志。
	//	b. 2 是隔离的，1 是删除的 2。当 2 重新加入集群时，2 将
	//	将过时的 MsgRequestVote 发送给 1 和 3，此时我们应该将 2 告诉 gc 本身。
	//	c. 2 是隔离的，但可以与 3 通信。1 删除 3。
	//	2 将向 3 发送过时的 MsgRequestVote，3 应忽略此消息。
	//	d. 2 是隔离的，但可以与 3 通信。1 删除 2，然后添加 4，删除 3。
	//	2 会向 3 发送过时的 MsgRequestVote，3 会将 2 告诉 gc 本身。
	//	e. 2 是隔离的。1 添加 4、5、6，删除 3、1。现在假设 4 是领导者。
	//	2 重新加入群集后，2 可能会向 1 和 3 发送过时的 MsgRequestVote，
	//	1 和 3 将忽略此消息。稍后 4 将向 2 发送消息，2 将发送消息
	//	再次重新加入 Raft 组。
	//  f. 2 是隔离的。1 添加 4、5、6，删除 3、1。现在假设 4 是领导者，而 4 删除了 2。
	//	与案例 E 不同，2 将永远过时。
	//	TODO：对于情况 f，如果 2 长时间过时，则 2 将与调度器通信，调度器将
	//	告诉 2 是过时的，所以 2 可以自行删除。
	region := d.Region()
	// 如果消息的Epoch小于当前region的epoch或者在当前region中找不到peer 则消息已过时且不在当前区域中 进行处理
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		// 消息已过时且不在当前区域中
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	//删除router中的peer
	d.ctx.router.close(regionID)
	//更改peer的状态
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	//log.Infof("newCompactLogRequest")
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
	log.Infof("onPrepareSplitRegion")
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerMsgHandler) process(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}
	if len(msg.Requests) == 0 {
		if msg.AdminRequest != nil {
			req := msg.AdminRequest
			switch req.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				compactLog := req.GetCompactLog()
				if compactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
					d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
					d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
					kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					d.ScheduleCompactLog(compactLog.CompactIndex)
				}
			case raft_cmdpb.AdminCmdType_Split:
				newRegionId := req.Split.NewRegionId
				newPeerIds := req.Split.NewPeerIds
				splitKey := req.Split.SplitKey

				// 判断msg的region是不是这个region
				if msg.Header.RegionId != d.regionId {
					regionNotFound := &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
					resp := ErrResp(regionNotFound)
					d.handleProposal(entry, resp)
					log.Infof("Region %d peer %d failed ErrRegionNotFound check", d.regionId, d.PeerId())
					return
				}

				//判断splitkey是否在当前region
				if err := util.CheckKeyInRegion(splitKey, d.Region()); err != nil {
					d.handleProposal(entry, ErrResp(err))
					log.Infof("Region %d peer %d failed KeyInRegion check err:%s", d.regionId, d.PeerId(), err.Error())
					return
				}

				if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
					if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
						siblingRegion := d.findSiblingRegion()
						if siblingRegion != nil {
							errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
						}
						d.handleProposal(entry, ErrResp(errEpochNotMatching))
						log.Infof("CheckRegionEpoch failed err:%s", err.Error())
						return
					}
				}

				//当raft层通过共识以后，这边需要做的是，
				// 1.排序 确保生成的新region中peer的顺序是一致的 方便split
				length := len(d.Region().Peers)
				// sort to ensure the order between different peers
				for i := 0; i < length; i++ {
					for j := 0; j < length-i-1; j++ {
						if d.Region().Peers[j].Id > d.Region().Peers[j+1].Id {
							temp := d.Region().Peers[j+1]
							d.Region().Peers[j+1] = d.Region().Peers[j]
							d.Region().Peers[j] = temp
						}
					}
				}
				// 3.新建一个peer，创建相关的元信息，注册到router中
				peers := []*metapb.Peer{}
				for i, p := range d.Region().Peers {
					peers = append(peers, &metapb.Peer{
						Id:      newPeerIds[i],
						StoreId: p.StoreId,
					})
				}
				region := &metapb.Region{
					Id:       newRegionId,
					StartKey: splitKey,
					EndKey:   d.Region().EndKey,
					RegionEpoch: &metapb.RegionEpoch{
						ConfVer: 1,
						Version: 1,
					},
					Peers: peers,
				}
				newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.splitCheckTaskSender, d.ctx.engine, region)
				if err != nil {
					log.Infof("createPeer err:%s", err.Error())
					return
				}
				//注册
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
				d.ctx.storeMeta.regions[newRegionId] = region

				// 2.原来的继承拆分前的元数据，修改Range 和 RegionEpoch
				d.Region().RegionEpoch.Version++
				d.Region().EndKey = splitKey
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{d.Region()})
				d.ctx.storeMeta.Unlock()

				meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
				meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
				//启动
				d.ctx.router.register(newPeer)
				d.ctx.router.send(newRegionId, message.Msg{RegionID: newRegionId, Type: message.MsgTypeStart})

				//callback 将拆分好的两个region返回
				resp := &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType: raft_cmdpb.AdminCmdType_Split,
						Split: &raft_cmdpb.SplitResponse{
							Regions: []*metapb.Region{d.Region(), region},
						},
					},
				}

				d.handleProposal(entry, resp)
				log.Infof("split done")
			}
		}
	}
	if len(msg.Requests) > 0 {
		//log.Infof("Requests:%v", msg.Requests[0])
		req := msg.Requests[0]
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			//log.Infof("delete key:%s", string(req.Delete.Key))
		case raft_cmdpb.CmdType_Snap:

		}

		if len(d.proposals) > 0 {
			p := d.proposals[0]

			for p.index < entry.Index {
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				d.proposals = d.proposals[1:]
				if len(d.proposals) == 0 {
					//log.Infof("nilllll")
					return
				}
				p = d.proposals[0]
			}

			if p.index == entry.Index {
				if p.term != entry.Term {
					//log.Infof(" p.term != entry.Term")
					NotifyStaleReq(entry.Term, p.cb)
				} else {
					resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}

					switch req.CmdType {
					case raft_cmdpb.CmdType_Get:
						value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
						if err != nil {
							p.cb.Done(ErrResp(err))
							return
						}
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get,
							Get: &raft_cmdpb.GetResponse{Value: value}}}
					case raft_cmdpb.CmdType_Put:
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put,
							Put: &raft_cmdpb.PutResponse{}}}
					case raft_cmdpb.CmdType_Delete:
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete,
							Delete: &raft_cmdpb.DeleteResponse{}}}
					case raft_cmdpb.CmdType_Snap:
						if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
							p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
							//log.Infof("msg.Header.RegionEpoch.Version")
							return
						}
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap,
							Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
						p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
						//log.Infof("Region:%v", d.Region())
					}
					p.cb.Done(resp)
					//log.Infof("resp")
				}
				d.proposals = d.proposals[1:]
			}
		}
	}
}

func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	//在ProposeConfChange函数中pb.ConfChange序列化到entry的data中
	cc := &eraftpb.ConfChange{}
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}
	//在proposeRaftCommand函数中将msg序列化到ConfChange的ctx中
	msg := *&raft_cmdpb.RaftCmdRequest{}
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	//在raft先增加或者移除节点
	d.RaftGroup.ApplyConfChange(*cc)

	if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
		//已经存在
		if d.isNodeExist(cc.NodeId) {
			return
		}

		//创建新的peer
		newPeer := &metapb.Peer{
			Id:      cc.NodeId,
			StoreId: msg.AdminRequest.ChangePeer.Peer.StoreId,
		}
		//log.Infof("add:%d", cc.NodeId)
		//添加到region中并且更改RegionEpoch
		d.Region().Peers = append(d.Region().Peers, newPeer)
		d.Region().RegionEpoch.ConfVer++
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
	} else if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		//如果是本身删除自身
		//log.Infof("remove:%d", cc.NodeId)
		if cc.NodeId == d.PeerId() {
			kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
			d.destroyPeer()
			return
		}

		//如果不是本身，从自己的region中删除
		if d.isNodeExist(cc.NodeId) {
			d.Region().RegionEpoch.ConfVer++
			deleteFromRegion(d.Region(), cc.NodeId)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			d.removePeerCache(cc.NodeId)
		}

	}

	//还有msg要处理
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		}}

	d.handleProposal(entry, resp)
}

func (d *peerMsgHandler) handleProposal(entry *pb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	if len(d.proposals) > 0 {
		p := d.proposals[0]

		for p.index < entry.Index {
			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			d.proposals = d.proposals[1:]
			if len(d.proposals) == 0 {
				//log.Infof("nilllll")
				return
			}
			p = d.proposals[0]
		}

		if entry.Index == p.index {
			if entry.Term != p.term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				p.cb.Done(resp)
			}
			d.proposals = d.proposals[1:]
		}
	}
}

func deleteFromRegion(region *metapb.Region, id uint64) {
	for i, p := range region.Peers {
		if p.Id == id {
			region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
			return
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func (d *peerMsgHandler) isNodeExist(id uint64) bool {
	for _, p := range d.Region().Peers {
		if p.GetId() == id {
			return true
		}
	}

	return false
}
