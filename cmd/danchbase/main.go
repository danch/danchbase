package main

import (
	"fmt"

	"github.com/danch/danchbase/net"
	"github.com/danch/danchbase/process"
	"github.com/danch/danchbase/store"
	"github.com/danch/danchbase/meta"
	"github.com/danch/danchbase/meta/reg"
	"github.com/danch/danchbase/com"
)

const reqQueue = 16
const maxInFlight = 16

func init() {
	//dummy up a registry
	table := meta.NewTable("notimplemented", 32767)
	store := store.NewStore(table, "", "")
	reg.Register(table, store)
}

func main() {
	var semChan = make(chan int, maxInFlight)
	var processChan = make(chan com.RequestContext, reqQueue)
	net.Listen("1720", processChan)

	fmt.Println("danchbase v0.0.1 is Go!")

	for true {
		req := <-processChan
		semChan <- 1
		go func(req com.RequestContext) {
			process.Process(req)
			<-semChan
		}(req)
	}
}
