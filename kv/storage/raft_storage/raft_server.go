package raft_storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/errors"
)

// RaftStorage is an implementation of `Storage` (see tikv/server.go) backed by a Raft node. It is part of a Raft network.
// By using Raft, reads and writes are consistent with other nodes in the TinyKV instance.
type RaftStorage struct {
	engines *engine_util.Engines //badger存储
	config  *config.Config

	node        *raftstore.Node
	snapManager *snap.SnapManager          //快照管理
	raftRouter  *raftstore.RaftstoreRouter //发送和存储消息
	raftSystem  *raftstore.Raftstore

	resolveWorker *worker.Worker
	snapWorker    *worker.Worker

	wg sync.WaitGroup
}

type RegionError struct {
	RequestErr *errorpb.Error
}

func (re *RegionError) Error() string {
	return re.RequestErr.String()
}

// 检查合法性
func (rs *RaftStorage) checkResponse(resp *raft_cmdpb.RaftCmdResponse, reqCount int) error {
	if resp.Header.Error != nil {
		return &RegionError{RequestErr: resp.Header.Error}
	}
	if len(resp.Responses) != reqCount {
		return errors.Errorf("responses count %d is not equal to requests count %d",
			len(resp.Responses), reqCount)
	}
	return nil
}

// NewRaftStorage creates a new storage engine backed by a raftstore.
// 新建存储
func NewRaftStorage(conf *config.Config) *RaftStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	raftDB := engine_util.CreateDB(raftPath, true)
	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)

	return &RaftStorage{engines: engines, config: conf}
}

func (rs *RaftStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// 用来存经过处理后的batch请求
	var reqs []*raft_cmdpb.Request
	// 批量处理请求
	for _, m := range batch {
		//判断请求的类型
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Put,
				Put: &raft_cmdpb.PutRequest{
					Cf:    put.Cf,
					Key:   put.Key,
					Value: put.Value,
				}})
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteRequest{
					Cf:  delete.Cf,
					Key: delete.Key,
				}})
		}
	}

	//将ctx中存储的数据封装到RaftRequestHeader中
	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header:   header,
		Requests: reqs,
	}

	//新建一个chan
	cb := message.NewCallback()
	//通过RaftStorage的router将请求发送到router的chan中
	if err := rs.raftRouter.SendRaftCommand(request, cb); err != nil {
		return err
	}

	// 将cb中的请求拿出来进行检查
	return rs.checkResponse(cb.WaitResp(), len(reqs))
}

func (rs *RaftStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	//封装所在位置消息
	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raft_cmdpb.Request{{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapRequest{},
		}},
	}

	//向router中发送请求
	cb := message.NewCallback()
	if err := rs.raftRouter.SendRaftCommand(request, cb); err != nil {
		return nil, err
	}

	//取出请求并检查是否有问题
	resp := cb.WaitResp()
	if err := rs.checkResponse(resp, 1); err != nil {
		if cb.Txn != nil {
			cb.Txn.Discard()
		}
		return nil, err
	}
	if cb.Txn == nil {
		panic("can not found region snap")
	}

	//没问题返回一个新的reader
	return NewRegionReader(cb.Txn, *resp.Responses[0].GetSnap().Region), nil
}

func (rs *RaftStorage) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	// 持续从TinyKv_RaftServer中接受消息并发送给rs.raftRouter
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		rs.raftRouter.SendRaftMessage(msg)
	}
}

func (rs *RaftStorage) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	var err error
	done := make(chan struct{})
	rs.snapWorker.Sender() <- &recvSnapTask{
		stream: stream,
		callback: func(e error) {
			err = e
			close(done)
		},
	}
	<-done
	return err
}

func (rs *RaftStorage) Start() error {
	cfg := rs.config
	schedulerClient, err := scheduler_client.NewClient(strings.Split(cfg.SchedulerAddr, ","), "")
	if err != nil {
		return err
	}

	rs.raftRouter, rs.raftSystem = raftstore.CreateRaftstore(cfg)

	rs.resolveWorker = worker.NewWorker("resolver", &rs.wg)
	resolveSender := rs.resolveWorker.Sender()
	resolveRunner := newResolverRunner(schedulerClient)
	rs.resolveWorker.Start(resolveRunner)
	rs.snapManager = snap.NewSnapManager(filepath.Join(cfg.DBPath, "snap"))
	rs.snapWorker = worker.NewWorker("snap-worker", &rs.wg)
	snapSender := rs.snapWorker.Sender()
	snapRunner := newSnapRunner(rs.snapManager, rs.config, rs.raftRouter)
	rs.snapWorker.Start(snapRunner)

	raftClient := newRaftClient(cfg)
	//开始lab2b(step1)
	trans := NewServerTransport(raftClient, snapSender, rs.raftRouter, resolveSender)

	rs.node = raftstore.NewNode(rs.raftSystem, rs.config, schedulerClient)
	//开始lab2b(step1)
	err = rs.node.Start(context.TODO(), rs.engines, trans, rs.snapManager)
	if err != nil {
		return err
	}

	return nil
}

func (rs *RaftStorage) Stop() error {
	rs.snapWorker.Stop()
	rs.node.Stop()
	rs.resolveWorker.Stop()
	rs.wg.Wait()
	if err := rs.engines.Raft.Close(); err != nil {
		return err
	}
	if err := rs.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}
