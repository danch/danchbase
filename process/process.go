package process

import (
	"fmt"

	"github.com/danch/danchbase/pb"
	"github.com/danch/danchbase/meta"
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
		record := meta.NewRecord(request.GetStartKey(), ctx.Data())
		err := store.Put(record)
		if (err != nil) {
			err = sendReply(ctx, pb.DBReply_InternalError)
		}
		err = sendReply(ctx, pb.DBReply_Success)
	case request.GetVerb() == pb.DBRequest_Get:
		fmt.Println("Recieved request")
		err = sendReply(ctx, pb.DBReply_OK)
	}
}

func sendReply(ctx com.RequestContext, status pb.DBReply_Status) error {
	var reply = new(pb.DBReply)
	reply.Status = status
	err := ctx.Send(reply)
	return err
}