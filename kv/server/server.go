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
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	lockBytes, err := reader.GetCF(engine_util.CfLock, req.Key)
	if err != nil {
		return nil, err
	}

	lockVersion := binary.BigEndian.Uint64(lockBytes[2:])
	if req.Version > lockVersion {
		return &kvrpcpb.GetResponse{
			RegionError: nil,
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.Key,
					LockVersion: lockVersion,
					Key:         req.Key,
					LockTtl:     0,
				},
				Retryable: "",
				Abort:     "",
				Conflict:  nil,
			},
			Value:    nil,
			NotFound: false,
		}, nil
	}

	var value []byte
	var ts uint64
	var val []byte
	iterator := reader.IterCF(engine_util.CfWrite)
	for ; iterator.Valid(); iterator.Next() {
		endTs := decodeTimestamp(iterator.Item().Key())
		value, err = iterator.Item().Value()
		if err != nil {
			return nil, err
		}

		if len(value) == 0 {
			return &kvrpcpb.GetResponse{
				RegionError: nil,
				Error:       nil,
				Value:       nil,
				NotFound:    true,
			}, nil
		}
		ts = binary.BigEndian.Uint64(value[1:])
		if req.Version >= endTs {
			val, err = reader.GetCF(engine_util.CfDefault, mvcc.EncodeKey(req.Key, ts))
			break
		}
	}

	if ts > req.Version {
		return &kvrpcpb.GetResponse{
			RegionError: nil,
			Error:       nil,
			Value:       nil,
			NotFound:    true,
		}, nil
	}

	resp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Error:       nil,
		Value:       val,
		NotFound:    false,
	}
	if len(val) == 0 {
		resp.NotFound = true
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	//log.Infof("a")

	for _, mutation := range req.Mutations {

		switch mutation.Op {
		case kvrpcpb.Op_Put:
			//尝试从latch那边获取锁
			waitGroup := server.Latches.AcquireLatches([][]byte{mutation.Key})
			if waitGroup != nil {
				return &kvrpcpb.PrewriteResponse{
					RegionError: nil,
					Errors: []*kvrpcpb.KeyError{{
						Locked:    nil,
						Retryable: "",
						Abort:     "",
						Conflict:  nil,
					},
					},
				}, nil
			}

			reader, err := server.storage.Reader(req.Context)
			if err != nil {
				return nil, err
			}

			//看看是否有没有提交的 如果有 比较ts
			iterator := reader.IterCF(engine_util.CfWrite)
			iterator.Seek(mutation.Key)
			if len(iterator.Item().Key()) != 0 {
				timestamp := decodeTimestamp(iterator.Item().Key())
				value, err := iterator.Item().Value()
				if err != nil {
					return nil, err
				}
				startTs := binary.BigEndian.Uint64(value[1:])
				//事务还没有结束 不允许修改
				if timestamp > req.StartVersion {
					return &kvrpcpb.PrewriteResponse{
						Errors: []*kvrpcpb.KeyError{{
							Conflict: &kvrpcpb.WriteConflict{
								StartTs:    startTs,
								ConflictTs: timestamp,
								Key:        mutation.Key,
								Primary:    nil,
							},
						},
						},
					}, nil
				}
			}

			//进行修改
			err = server.storage.Write(req.Context, []storage.Modify{{storage.Put{
				Key:   mvcc.EncodeKey(mutation.Key, req.StartVersion),
				Value: mutation.Value,
				Cf:    engine_util.CfDefault,
			}}})
			if err != nil {
				return nil, err
			}

			//写入cflock
			val := make([]byte, 18)
			val[0] = 1
			val[1] = 1
			binary.BigEndian.PutUint64(val[2:], req.StartVersion)
			binary.BigEndian.PutUint64(val[10:], req.LockTtl)
			server.storage.Write(req.Context, []storage.Modify{{storage.Put{
				Key:   mutation.Key,
				Value: val,
				Cf:    engine_util.CfLock,
			}}})
		case kvrpcpb.Op_Del:
			for _, mutation := range req.Mutations {
				server.storage.Write(req.Context, []storage.Modify{{
					&storage.Delete{
						Key: mutation.Key,
						Cf:  engine_util.CfDefault,
					}},
				})
			}
		}
	}

	resp := &kvrpcpb.PrewriteResponse{
		RegionError: nil,
		Errors:      nil,
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
