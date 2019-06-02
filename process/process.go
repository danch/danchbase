package process

import (
	"github.com/danch/danchbase/pb"
	"github.com/danch/danchbase/meta/reg"
	"github.com/danch/danchbase/com"
)

//Process a request
func Process(ctx com.RequestContext) {
	request := ctx.Request()
	store, err := reg.GetStore(request.GetTableName(), request.GetStartKey())
	if (err != nil) {
		reply := pb.DBReply{}
		ctx.Send(&reply)
	}
	switch {
	case request.GetVerb() == pb.DBRequest_Put:
		record := pb.NewRecord(request.GetStartKey(), ctx.Data())
		err := store.Put(record)
		if (err != nil) {
			err = sendReply(ctx, pb.DBReply_InternalError, nil)
		}
		err = sendReply(ctx, pb.DBReply_Success, nil)
	case request.GetVerb() == pb.DBRequest_Get:
		record, err := store.Get(request.GetStartKey())
		if (err != nil) {
			sendReply(ctx, pb.DBReply_InternalError, nil)
		}
		err = sendReply(ctx, pb.DBReply_OK, record)
	}
}

func sendReply(ctx com.RequestContext, status pb.DBReply_Status, record *pb.Record) error {
	var reply = pb.NewReply(status, record)
	err := ctx.Send(reply)
	return err
}