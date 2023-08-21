package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Engine      *badger.DB
	AloneReader *StandAloneReader
}

type StandAloneReader struct {
	Iter *engine_util.BadgerIterator
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		Engine:      engine_util.CreateDB(conf.DBPath, conf.Raft),
		AloneReader: new(StandAloneReader),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.Engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s.AloneReader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			s.Engine.Update(func(txn *badger.Txn) error {
				if err := txn.Set(modify.Key(), modify.Value()); err != nil {
					return err
				}
				return nil
			})
		case storage.Delete:
			s.Engine.Update(func(txn *badger.Txn) error {
				if err := txn.Delete(modify.Key()); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return nil
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {

	return nil, nil
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	txn := &badger.Txn{}
	return engine_util.NewCFIterator(cf, txn)
}

func (r *StandAloneReader) Close() {
	r.Iter.Close()
}
