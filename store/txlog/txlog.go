package txlog

import (
	"io"
	"os"
	"time"
	"strconv"
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/danch/danchbase/config"
	"github.com/danch/danchbase/pb"
)

//TxLogError is an error from the TxLog
type TxLogError struct {
	message string
}
func (err TxLogError) Error() string {
	return err.message
}

//TxLog exposes functions controlling the transaction log
type TxLog interface {
	RecordTransaction(record *pb.Record) error
	FilePath() string
	StoreName() string
}
func createFile(dirname, filename string) (*os.File, error) {
	//TODO ensure directory exists
	return os.OpenFile(dirname + "/" + filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
}

//NewTransactionLog creates an empty transaction log
func NewTransactionLog(storeName string) (TxLog, error) {
	conf := config.GetConfig()
	var filename = storeName + strconv.FormatInt(time.Now().Unix(), 10)
	file, err := createFile(conf.TxLogsDir , filename)
	if (err != nil) {
		return nil, err
	}
	var txLog = new(txLog)
	txLog.storeName = storeName
	txLog.storePath = file.Name()
	txLog.file = file
	return txLog, nil
}

//Recover the transaction log contained in the given file
func Recover(filename string, recoveryChannel chan *pb.Record) (TxLog, error) {
	conf := config.GetConfig()
	file, err := os.OpenFile(conf.TxLogsDir + "/" + filename, os.O_RDWR, 0644)
	if (err != nil) {
		return nil, err
	}
	var txLog = new(txLog)
	txLog.storeName = "RECOVERY"
	txLog.file = file
	go txLog.recoverFrom(file, recoveryChannel)
	return txLog, nil
}

func (log *txLog) recoverFrom(file *os.File, recoveryChannel chan *pb.Record) {
	defer close(recoveryChannel)
	//TODO log error conditions, improve error propogation (probably make the channel its own struct)
	for {
		var size uint32
		err := binary.Read(file, binary.LittleEndian, &size)
		if err != nil {
			break
		}
		var buff = make([]byte, size)
		count, err := file.Read(buff)
		if err != nil || count != int(size) {
			break
		}
		var record = new(pb.Record)
		err = proto.Unmarshal(buff, record)
		if (err != nil) {
			break
		}
		recoveryChannel <- record
	}
	file.Seek(0, io.SeekEnd)
}

type txLog struct {
	storeName string
	storePath string
	file *os.File
}

func (log *txLog) FilePath() string {
	return log.storePath
}
func (log *txLog) StoreName() string {
	return log.storeName
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
