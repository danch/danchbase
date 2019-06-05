package main

import (
	"fmt"
	"io/ioutil"

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

func dummyInit() error {
	//see if we have any transaction logs
	files, err := ioutil.ReadDir(config.GetConfig().TxLogsDir)
	if (err != nil) {
		return err
	}
	if (len(files) > 0) {
		for _, file := range files {
			if !file.IsDir() {
				store, err := store.Recover(file.Name())
				if (err != nil) {
					return err
				}
				reg.Register(store.Table(), store)
			}
		}
		return nil
	}
	//dummy up a registry
	table := meta.NewTable("notimplemented", 32767)
	store, err := store.NewStore(table, "region1", "", "")
	if (err != nil) {
		panic(err)
	}
	reg.Register(table, store)

	return nil
}

func main() {

	conf, err := config.Configure("./local-config.json")
	if (err != nil) {
		panic(err)
	}

	err = dummyInit()
	if (err != nil) {
		panic(err)
	}

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
