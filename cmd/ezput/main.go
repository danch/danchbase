package main

import (
	"fmt"

	"github.com/danch/danchbase/client"
	"github.com/danch/danchbase/pb"
	"github.com/google/uuid"
)

func main() {
	var client, err = client.NewClient("localhost:1720")
	if err != nil {
		fmt.Println("Error connecting to server " + err.Error())
		return
	}
	defer client.Close()

	key := uuid.New()
	value := "This is a test of the random data system"
	reply, err := client.Put(key.String(), []byte(value))
	if err != nil {
		fmt.Println("Error calling Put " + err.Error())
		return
	}
	fmt.Println("Request was "+pb.DBReply_Status_name[int32(reply.GetStatus())])

	fmt.Println("Trying to fetch back the same record")
	reply, err = client.Get(key.String())
	if (err != nil) {
		fmt.Println("Error in Get "+err.Error())
		return
	}
	fmt.Println("Received record. key="+reply.GetRecord().GetKey()+" value="+string(reply.GetRecord().GetData()))
}
