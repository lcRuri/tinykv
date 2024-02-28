package server

import (
	"context"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).
//下面的函数是服务器的gRPC API(实现TinyKvServer)

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Error:       nil,
		Value:       nil,
		NotFound:    false,
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	//获取这个key对呀事务的锁
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}

	//锁记录的是可以看到的ts key 被事务锁住 getlcok
	if lock != nil && req.Version >= lock.Ts {
		resp.Error = &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
			PrimaryLock: lock.Primary,
			LockVersion: lock.Ts,
			Key:         req.Key,
			LockTtl:     lock.Ttl,
		}}
		return resp, nil
	}

	//读取值
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if len(value) == 0 {
		resp.NotFound = true
	}

	resp.Value = value

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}
	defer reader.Close()
	//创建事务
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	var keyErrors []*kvrpcpb.KeyError
	//遍历操作进行判断执行
	for _, mutation := range req.Mutations {
		//here
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		//存在事务 同时请求的时间在这个事务中间
		if write != nil && req.StartVersion < ts {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		//here !=
		if lock != nil && lock.Ts != req.StartVersion {
			return &kvrpcpb.PrewriteResponse{
				Errors: []*kvrpcpb.KeyError{{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    req.StartVersion,
						ConflictTs: lock.Ts,
						Key:        mutation.Key,
						Primary:    lock.Primary,
					},
				},
				},
			}, nil
		}

		var kind mvcc.WriteKind
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
			txn.DeleteValue(mutation.Key)
		case kvrpcpb.Op_Rollback:
		}

		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}

	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
		return resp, nil
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		write, _, err := txn.MostRecentWrite(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		if write != nil {
			//存在write并且write的值意味着rollback
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Retryable: "false"}
				return resp, nil
			}

			if write.StartTS == req.StartVersion {
				continue
			}

		}

		lock, err := txn.GetLock(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		//缺失了cflock lock相当于二阶段提交的第一阶段的凭证 没有的说明第一阶段没锁住
		if lock == nil {
			continue
		}

		if lock != nil {
			if lock.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
				return resp, nil
			}
		}

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindPut,
		})

		txn.DeleteLock(key)

	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}

	return resp, nil

}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	//获取锁
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}

	//没有lock 可能是已经提交了 也有可能是回滚了
	if lock == nil {
		write, endTs, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		//没有write 说明是回滚了 写入一条write why
		if write == nil {
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
		} else {
			//存在write kind不为rollback 说明已经提交了 返回提交时间
			if write.Kind != mvcc.WriteKindRollback {
				resp.CommitVersion = endTs
			}
		}
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		return resp, nil
	}

	//锁超时
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		//删除锁 删除值 写入一条write
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		//why is lockTs
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: lock.Ts,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		//先判断这个key有没有存在的write
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		//存在并且已经是rollback 跳过处理下一个key
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}

		if write != nil && write.Kind == mvcc.WriteKindPut {
			resp.Error = &kvrpcpb.KeyError{Abort: "true"}
			return resp, nil
		}

		//判断是不是有锁
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
		}

		//有锁而且不是本次事务的锁
		if lock != nil && lock.Ts != req.StartVersion {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}

		//前置检查完毕 删除之前的值并且写入一个rollback的write
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})

		txn.DeleteValue(key)
		if lock != nil {
			txn.DeleteLock(key)
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iterator := txn.Reader.IterCF(engine_util.CfLock)
	defer iterator.Close()
	for ; iterator.Valid(); iterator.Next() {
		key := iterator.Item().Key()

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			continue
		}

		if lock.Ts == req.StartVersion && req.CommitVersion != 0 {
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindPut,
			})
			txn.DeleteLock(key)
		} else if lock.Ts == req.StartVersion && req.CommitVersion == 0 {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			txn.DeleteLock(key)
			txn.DeleteValue(key)

		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}
