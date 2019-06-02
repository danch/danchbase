package main

import (
	"fmt"

	"github.com/danch/danchbase/net"
	"github.com/danch/danchbase/pb"
	"github.com/danch/danchbase/process"
)

const reqQueue = 16
const maxInFlight = 16

func main() {
	var semChan = make(chan int, maxInFlight)
	var processChan = make(chan *pb.DBRequest, reqQueue)
	net.Listen("1720", processChan)

	fmt.Println("danchbase v0.0.1 is Go!")

	for true {
		req := <-processChan
		semChan <- 1
		go func(req *pb.DBRequest) {
			process.Process(req)
			<-semChan
		}(req)
	}
}
