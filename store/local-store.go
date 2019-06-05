package store

import (
	"container/list"
	"strings"

	"github.com/danch/danchbase/pb"
	"github.com/danch/danchbase/meta"
	"github.com/danch/danchbase/store/txlog"
)

type StoreNotReady struct {
}
func (StoreNotReady) Error() string {return "Store not ready"}
// LocalStore a storage managed by this process
type LocalStore struct {
	table       *meta.Table
	regionID 	string
	regionStart string
	regionEnd   string
	ready		bool

	currentSegment *list.List
	currentTxLog txlog.TxLog
}

// NewStore creates a store for the region of table starting with
// startKey (inclusive) and extending to endKey (exclusive)
func NewStore(table *meta.Table, regionID, startKey, endKey string) (*LocalStore, error) {
	s := new(LocalStore)
	s.table = table
	s.regionID = regionID
	s.regionStart = startKey
	s.regionEnd = endKey
	s.currentSegment = list.New()
	log, err := txlog.NewTransactionLog(regionID)
	if err != nil {
		return nil, err
	}
	s.currentTxLog = log
	s.ready = true
	return s, nil
}
const recoverQueueSize = 50
//Recover from the transaction log at the indicated location
func Recover(txFilePath string) (*LocalStore, error) {
	txChannel := make(chan *pb.Record, recoverQueueSize)
	log, err := txlog.Recover(txFilePath, txChannel)
	if err != nil {
		return nil, err
	}
	store := new(LocalStore)
	//when we get to for realsies, we'll have metadata
	store.table = meta.NewTable("unknown", 32767)
	store.regionID = "unknown"
	store.regionStart = ""
	store.regionEnd = ""
	store.ready = false
	store.currentSegment = list.New()
	store.currentTxLog = log
	
	go store.recoverTransactions(txFilePath, txChannel)
	return store, nil
}
func (store *LocalStore) recoverTransactions(txFilePath string, ch chan *pb.Record) {

	for {
		rec := <- ch
		if rec == nil {
			break;
		}
		store.insertCurrent(rec)
	}
	store.ready = true
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
	if !store.ready {
		return StoreNotReady{}
	}
	store.currentTxLog.RecordTransaction(record)

	store.insertCurrent(record)
	return nil
}

func (store *LocalStore) insertCurrent(record *pb.Record) {
	if store.currentSegment.Front() == nil {
		store.currentSegment.PushFront(record)
		return
	}
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*pb.Record)
		if strings.Compare(r.GetKey(), record.GetKey()) > 0 {
			store.currentSegment.InsertBefore(record, e)
			return
		}
	}
	store.currentSegment.PushBack(record)
}

// Get the record with the key, or nil of not found
func (store *LocalStore) Get(key string) (*pb.Record, error) {
	if !store.ready {
		return nil, StoreNotReady{}
	}
	//linear search for now
	for e := store.currentSegment.Front(); e != nil; e = e.Next() {
		var r = e.Value.(*pb.Record)
		if strings.Compare(r.GetKey(), key) == 0 {
			return r, nil
		}
	}
	return nil, nil
}

func (store *LocalStore) Table() *meta.Table {
	return store.table
}
func (store *LocalStore) RegionID() string {
	return store.regionID
}