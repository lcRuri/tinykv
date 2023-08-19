package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"net/http"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	StoreAddr     string
	SchedulerAddr string
	LogLevel      string
	DBPath        string
	CFMap         map[string]*os.File
	Read          StandAloneStorageReader
	server        http.Server
}

type StandAloneStorageReader struct {
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		StoreAddr:     conf.StoreAddr,
		SchedulerAddr: conf.SchedulerAddr,
		LogLevel:      conf.LogLevel,
		DBPath:        conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

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
