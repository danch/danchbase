package store

import (
	"container/list"
	"strings"

	"github.com/danch/danchbase/pb"
	"github.com/danch/danchbase/meta"
)


// LocalStore a storage managed by this process
type LocalStore struct {
	table       *meta.Table
	regionId 	string
	regionStart string
	regionEnd   string

	currentSegment *list.List
	currentTxLog TxLog
}

// NewStore creates a store for the region of table starting with
// startKey (inclusive) and extending to endKey (exclusive)
func NewStore(table *meta.Table, regionId, startKey, endKey string) (*LocalStore, error) {
	s := new(LocalStore)
	s.table = table
	s.regionId = regionId
	s.regionStart = startKey
	s.regionEnd = endKey
	s.currentSegment = list.New()
	log, err := NewTransactionLog(regionId)
	if err != nil {
		return nil, err
	}
	s.currentTxLog = log
	return s, nil
}
//StartKey returns the start key for the store, or ""
func (store *LocalStore) StartKey() string {
	return store.regionStart
}
//EndKey returns the end key for the store, or ""
func (store *LocalStore) EndKey() string {
	return store.regionEnd
}

// Put upserts the record into the store
func (store *LocalStore) Put(record *pb.Record) error {
	store.currentTxLog.RecordTransaction(record)
	if store.currentSegment.Front() == nil {
		store.currentSegment.PushFront(record)
		return nil
	}
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*pb.Record)
		if strings.Compare(r.GetKey(), record.GetKey()) > 0 {
			store.currentSegment.InsertBefore(record, e)
			return nil
		}
	}
	store.currentSegment.PushBack(record)
	return nil
}

// Get the record with the key, or nil of not found
func (store *LocalStore) Get(key string) (*pb.Record, error) {
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*pb.Record)
		if strings.Compare(r.GetKey(), key) == 0 {
			return r, nil
		}
	}
	return nil, nil
}
