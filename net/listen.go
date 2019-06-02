package net

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/danch/danchbase/pb"
	"github.com/danch/danchbase/com"
)

var (
	listener net.Listener //only 1 port for now
)

//Error is an error from the networking code which may be recoverable or not
type Error struct {
	msg string
	recoverable bool
}
func (ne Error) Error() string {
	return ne.msg
}

type netRequestContext struct {
	request *pb.DBRequest
	data []byte
	connection net.Conn
}
func (ctx netRequestContext) Request() *pb.DBRequest {
	return ctx.request
}
func (ctx netRequestContext) Data() []byte {
	return ctx.data
}

func (ctx netRequestContext) Send(reply *pb.DBReply) error {
	var buff, err = proto.Marshal(reply)
	if err != nil {
		fmt.Println("Cant marshal reply message")
		panic(1)
	}
	var size uint32 = uint32(len(buff))
	binary.Write(ctx.connection, binary.LittleEndian, size)
	count, err := ctx.connection.Write(buff)
	if err != nil || count != len(buff) {
		fmt.Println("Error writing to client" + err.Error())
		return err
	}
	return nil
}

// Listen to the indicated host and port sending incoming DBRequests to the channel
func Listen(port string, queue chan com.RequestContext) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	go acceptLoop(listener, queue)
	return nil
}

func acceptLoop(listener net.Listener, queue chan com.RequestContext) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error in accept loop: " + err.Error())
		}
		go processLoop(conn, queue)
	}
}

func processLoop(conn net.Conn, queue chan com.RequestContext) {
	var errorChannel = make(chan bool)
	for {
		select {
		case cont := <-errorChannel:
			if !cont {
				fmt.Println("received stop indicator", cont)
				break
			}
		default:
		}

		var msgSize uint32
		err := binary.Read(conn, binary.LittleEndian, &msgSize)
		if err != nil {
			fmt.Println("Error reading msg size " + err.Error())
			break
		}
		buffer := make([]byte, msgSize)
		count, err := conn.Read(buffer)
		if ne := checkRead(uint32(count), err, msgSize); err != nil {
			go send(pb.DBReply_InvalidRequest, netRequestContext{nil, nil, conn}, errorChannel)
			if !ne.recoverable {
				break
			}
			continue
		}

		var request = new(pb.DBRequest)
		if err := proto.Unmarshal(buffer, request); err != nil {
			go send(pb.DBReply_InvalidRequest, netRequestContext{nil, nil, conn}, errorChannel)
			continue
		}
		var msgData []byte
		if request.Datalength > 0 {
			msgData = make([]byte, request.Datalength)
			count, err = conn.Read(msgData)
			if ne := checkRead(uint32(count), err, uint32(request.Datalength)); err != nil {
				go send(pb.DBReply_InvalidRequest, netRequestContext{nil, nil, conn}, errorChannel)
				if !ne.recoverable {
					break
				}
				continue
			}
		}

		queue <- netRequestContext{request, msgData, conn}
	}
	conn.Close()
}

func checkRead(count uint32, err error, msgSize uint32) *Error {
	if err != nil {
		return &Error{"Error reading from client " + err.Error(), false}
	}
	if uint32(count) != msgSize {
		return &Error{"Error reading from client " + err.Error(), true}
	}
	return nil
}

func send(status pb.DBReply_Status, ctx com.RequestContext, errorChannel chan bool) {
	var reply = new(pb.DBReply)
	reply.Status = status
	err := ctx.Send(reply)
	if (err != nil) {
		errorChannel <- false
	}
}
