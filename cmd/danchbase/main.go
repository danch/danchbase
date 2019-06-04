package main

import (
	"fmt"
	"os"

	"github.com/danch/danchbase/net"
	"github.com/danch/danchbase/process"
	"github.com/danch/danchbase/store"
	"github.com/danch/danchbase/meta"
	"github.com/danch/danchbase/meta/reg"
	"github.com/danch/danchbase/com"
	"github.com/danch/danchbase/config"
)

const reqQueue = 16
const maxInFlight = 16

func main() {

	curdir, _ := os.Getwd()
	fmt.Println(curdir)
	conf, err := config.Configure("./local-config.json")
	if (err != nil) {
		panic(err)
	}

	//dummy up a registry
	table := meta.NewTable("notimplemented", 32767)
	store, err := store.NewStore(table, "region1", "", "")
	if (err != nil) {
		panic(err)
	}
	reg.Register(table, store)

	var semChan = make(chan int, maxInFlight)
	var processChan = make(chan com.RequestContext, reqQueue)
	net.Listen(conf.Port, processChan)

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
