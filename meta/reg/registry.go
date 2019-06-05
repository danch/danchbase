package reg

import (
	"github.com/danch/danchbase/store"
	"github.com/danch/danchbase/meta"
)

//MetaStoreError returned from registry operations
type MetaStoreError struct {
	msg string
}
func (mse MetaStoreError) Error() string {
	return mse.msg
}
type regionMapEntry struct {
	table *meta.Table
	startKey string
	endKey string
	store store.Store
}
var (
	storeRegistry = map[string][]regionMapEntry{}
)

// GetStore returns the store for the given table/key
func GetStore(tablename, key string) (store.Store, error) {
	for _, v := range storeRegistry {
		if len(v) >  0 {
			return v[0].store, nil
		}
	}
	return nil, MetaStoreError{"Unknown table"}
	/*
	regionList := storeRegistry[tablename]
	if regionList == nil {
		return nil, MetaStoreError{"Unknown table"}
	}
	//TODO linear search for now
	for _, entry := range regionList {
		if (keyInRange(key, entry.startKey, entry.endKey)) {
			return entry.store, nil
		}
	}
	*/
	return nil, MetaStoreError{"Can't find store for key "+key+" in table '"+tablename+"'"}
}
// Register a store for a table
func Register(table *meta.Table, store store.Store) {
	//TODO: thread safety
	var storeSlice = storeRegistry[table.Name()]
	if (storeSlice == nil) {
		storeRegistry[table.Name()] = []regionMapEntry{regionMapEntry{table, store.StartKey(), store.EndKey(), store}}
	}
	storeRegistry[table.Name()] = append(storeSlice, regionMapEntry{table, store.StartKey(), store.EndKey(), store})
}

func keyInRange(key, startKey, endKey string) bool {
	return (key >= startKey || startKey == "") && (key < endKey || endKey == "")
}