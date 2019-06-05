package com

import (
	"github.com/danch/danchbase/pb"
)
//RequestContext is the context in which a request is being processed
type RequestContext interface {
	Request() *pb.DBRequest
	Data() []byte
	Send(*pb.DBReply) error
}