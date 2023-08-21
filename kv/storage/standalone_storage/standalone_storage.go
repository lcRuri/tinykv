package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
	"sync"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
	reader  *StandAloneReader
	wg      *sync.WaitGroup
}

type StandAloneReader struct {
	txn *badger.Txn
	db  *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	var sa = &StandAloneStorage{}
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")

	os.MkdirAll(kvPath, os.ModePerm)

	kvDB := engine_util.CreateDB(kvPath, false)
	sa.engines = engine_util.NewEngines(kvDB, nil, kvPath, "")
	sa.config = conf
	sa.wg = new(sync.WaitGroup)
	sa.reader = new(StandAloneReader)
	sa.reader.db = kvDB

	return sa
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.wg.Add(1)

	go func() {
		s.wg.Wait()
	}()
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	defer s.wg.Done()
	s.reader.Close()
	err := s.engines.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s.reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engines.Kv, modify.Cf(), modify.Key(), modify.Value())
			return err
		case storage.Delete:
			err := engine_util.DeleteCF(s.engines.Kv, modify.Cf(), modify.Key())
			return err
		}
	}
	return nil
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(r.db, cf, key)
	if err != nil {
		if err.Error() == "Key not found" {
			return nil, nil
		} else {
			return nil, err
		}

	}
	return val, nil
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	r.txn = r.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {

}
