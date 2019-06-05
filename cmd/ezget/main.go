package main

import (
	"fmt"
	"os"

	"github.com/danch/danchbase/client"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Need to provide at least one key")
	}
	keys := os.Args[1:]

	var client, err = client.NewClient("localhost:1720")
	if err != nil {
		fmt.Println("Error connecting to server " + err.Error())
		return
	}
	defer client.Close()

	for _, key := range keys {
		fmt.Println("Trying to fetch record with key " + key)
		reply, err := client.Get(key)
		if (err != nil) {
			fmt.Println("Error in Get "+err.Error())
			return
		}
		fmt.Println("Received record. key="+reply.GetRecord().GetKey()+" value="+string(reply.GetRecord().GetData()))
	}
}
