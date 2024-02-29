package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := &Scanner{
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
	scan.iter.Seek(EncodeKey(startKey, txn.StartTS))
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	key := scan.iter.Item().KeyCopy(nil)
	userKey := DecodeUserKey(key)

	//底层存储key是按照key的顺序来排的
	//这就可能导致当前key的ts超过txn的ts 但是之后的key的ts可能是满足条件的
	//于是需要我们按照当前txn的ts seek一下
	scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
	key1 := scan.iter.Item().KeyCopy(nil)
	decodeUserKey := DecodeUserKey(key1)
	//比较了不同之后 那么我们就需要继续递归 找到满足条件的
	if !bytes.Equal(decodeUserKey, userKey) {
		return scan.Next()
	}

	//这样得到的value才是满足ts的
	value, err := scan.iter.Item().ValueCopy(nil)
	if err != nil {
		return userKey, nil, err
	}

	//key被删除了
	if WriteKind(value[0]) == WriteKindDelete {
		return nil, nil, err
	}
	var ts uint64
	if value != nil {
		ts = binary.BigEndian.Uint64(value[1:])
	} else {
		return nil, nil, err
	}

	val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, ts))

	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		tmpKey := scan.iter.Item().KeyCopy(nil)
		tmpUserkey := DecodeUserKey(tmpKey)
		if !bytes.Equal(tmpUserkey, userKey) {
			break
		}

	}

	return userKey, val, err

}
