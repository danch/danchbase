package store

import (
	"github.com/danch/danchbase/pb"
)
// Store represents a store containing a contiguous key range within a table
type Store interface {
	Put(record *pb.Record) error
	Get(key string) (*pb.Record, error)
	StartKey() string
	EndKey() string
}