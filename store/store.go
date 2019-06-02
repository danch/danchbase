package store

import (
	"container/list"
	"strings"

	"github.com/danch/danchbase/meta"
)

// Store represents a store containing a contiguous key range within a table
type Store interface {
	Put(record *meta.Record) error
	Get(key string) (*meta.Record, error)
	StartKey() string
	EndKey() string
}

// LocalStore a storage managed by this process
type LocalStore struct {
	table       *meta.Table
	regionStart string
	regionEnd   string

	currentSegment *list.List
}

// NewStore creates a store for the region of table starting with
// startKey (inclusive) and extending to endKey (exclusive)
func NewStore(table *meta.Table, startKey, endKey string) *LocalStore {
	s := new(LocalStore)
	s.table = table
	s.regionStart = startKey
	s.regionEnd = endKey
	s.currentSegment = list.New()
	return s
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
func (store *LocalStore) Put(record *meta.Record) error {
	if store.currentSegment.Front() == nil {
		store.currentSegment.PushFront(record)
		return nil
	}
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*meta.Record)
		if strings.Compare(r.Key(), record.Key()) > 0 {
			store.currentSegment.InsertBefore(record, e)
			return nil
		}
	}
	store.currentSegment.PushBack(record)
	return nil
}

// Get the record with the key, or nil of not found
func (store *LocalStore) Get(key string) (*meta.Record, error) {
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*meta.Record)
		if strings.Compare(r.Key(), key) == 0 {
			return r, nil
		}
	}
	return nil, nil
}
