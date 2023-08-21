package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)

	if err != nil {
		if err.Error() == "Key not found" {
			return &kvrpcpb.RawGetResponse{Value: val, Error: err.Error()}, nil
		} else {
			return nil, err
		}

	}

	resp := &kvrpcpb.RawGetResponse{Value: val}
	if val == nil {
		resp.NotFound = true
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}

	batch := make([]storage.Modify, 0)
	batch = append(batch, modify)
	err := server.storage.Write(nil, batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, nil
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{Data: storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}}

	batch := make([]storage.Modify, 0)
	batch = append(batch, modify)
	err := server.storage.Write(nil, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	iterCF := reader.IterCF(req.Cf)

	for iterCF.Seek(req.StartKey); iterCF.Valid(); iterCF.Next() {
		key := iterCF.Item().Key()
		value, err := iterCF.Item().Value()
		if err != nil {
			return nil, err
		}
		pair := &kvrpcpb.KvPair{Key: key, Value: value}

		resp.Kvs = append(resp.Kvs, pair)

		if len(resp.Kvs) == int(req.Limit) {
			return resp, nil
		}
	}
	return resp, nil

}
