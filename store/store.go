package store

import (
	"github.com/danch/danchbase/meta"
	"github.com/danch/danchbase/pb"
)
// Store represents a store containing a contiguous key range within a table
type Store interface {
	Put(record *pb.Record) error
	Get(key string) (*pb.Record, error)
	Table() *meta.Table
	RegionID() string
	StartKey() string
	EndKey() string
}