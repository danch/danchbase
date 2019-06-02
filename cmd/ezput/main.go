package main

import (
	"fmt"

	"github.com/danch/danchbase/client"
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
	err = client.Put(key.String(), []byte(value))
	if err != nil {
		fmt.Println("Error calling Put " + err.Error())
	}
}
