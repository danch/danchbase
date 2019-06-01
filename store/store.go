package store

import (
	"container/list"
	"strings"

	"github.com/danch/danchbase/meta"
)

// Store represents a store containing a contiguous key range within a table
type Store struct {
	table       *meta.Table
	regionStart string
	regionEnd   string

	currentSegment *list.List
}

// NewStore creates a store for the region of table starting with
// startKey (inclusive) and extending to endKey (exclusive)
func NewStore(table *meta.Table, startKey, endKey string) *Store {
	s := new(Store)
	s.table = table
	s.regionStart = startKey
	s.regionEnd = endKey
	s.currentSegment = list.New()
	return s
}

// Add the record to the store
func (store *Store) Add(record *meta.Record) {
	if store.currentSegment.Front() == nil {
		store.currentSegment.PushFront(record)
		return
	}
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*meta.Record)
		if strings.Compare(r.Key(), record.Key()) > 0 {
			store.currentSegment.InsertBefore(record, e)
			return
		}
	}
	store.currentSegment.PushBack(record)
}

// Get the record with the key, or nil of not found
func (store *Store) Get(key string) *meta.Record {
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*meta.Record)
		if strings.Compare(r.Key(), key) == 0 {
			return r
		}
	}
	return nil
}
