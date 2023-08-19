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
	engine engine_util.Engines
}

type StandAloneStorageReader struct {
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		engine: engine_util.Engines{
			Kv:     new(badger.DB),
			KvPath: conf.DBPath,
		},
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			s.engine.Kv.Update(func(txn *badger.Txn) error {
				if err := txn.Set(modify.Key(), modify.Value()); err != nil {
					return err
				}
				return nil
			})
		case storage.Delete:
			s.engine.Kv.Update(func(txn *badger.Txn) error {
				if err := txn.Delete(modify.Key()); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return nil
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {

	return nil, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return nil
}

func (s *StandAloneStorageReader) Close() {

}
