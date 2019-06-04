package store

import (
	"os"
	"time"
	"strconv"
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/danch/danchbase/config"
	"github.com/danch/danchbase/pb"
)

//TxLog exposes functions controlling the transaction log
type TxLog interface {
	RecordTransaction(record *pb.Record) error
}
type txLog struct {
	storeName string
	file *os.File
	txCount int
}

type TxLogError struct {
	message string
}
func (err TxLogError) Error() string {
	return err.message
}

func createFile(dirname, filename string) (*os.File, error) {
	//TODO ensure directory exists
	return os.Create(dirname + "/" + filename)
}

//NewTransactionLog creates an empty transaction log
func NewTransactionLog(storeName string) (TxLog, error) {
	conf := config.GetConfig()
	var filename = storeName + strconv.FormatInt(time.Now().Unix(), 10)
	file, err := createFile(conf.DataDir , filename)
	if (err != nil) {
		return nil, err
	}
	var txLog = new(txLog)
	txLog.storeName = storeName
	txLog.file = file
	txLog.txCount = 0
	return txLog, nil
}

func (log *txLog) RecordTransaction(record *pb.Record) error {
	buff, err := proto.Marshal(record)
	if (err != nil) {
		return err
	}
	var size uint32 = uint32(len(buff))
	
	binary.Write(log.file, binary.LittleEndian, size)
	count, err := log.file.Write(buff)
	if err != nil || count != int(size) {
		return TxLogError{"Error writing to TxLog "+err.Error()}
	}
	return nil
}
