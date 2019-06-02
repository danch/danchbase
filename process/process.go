package process

import (
	"fmt"

	"github.com/danch/danchbase/pb"
)

//Process a request
func Process(request *pb.DBRequest) {
	switch {
	case request.GetVerb() == pb.DBRequest_Put:
		fmt.Println("Recieved request")
	case request.GetVerb() == pb.DBRequest_Get:
		fmt.Println("Recieved request")
	}
}
