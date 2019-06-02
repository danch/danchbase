package net

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/danch/danchbase/pb"
	"github.com/golang/protobuf/proto"
)

var (
	listener net.Listener //only 1 port for now
)

// Listen to the indicated host and port sending incoming DBRequests to the channel
func Listen(port string, queue chan *pb.DBRequest) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	go acceptLoop(listener, queue)
	return nil
}

func acceptLoop(listener net.Listener, queue chan *pb.DBRequest) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error in accept loop: " + err.Error())
		}
		go processLoop(conn, queue)
	}
}

func processLoop(conn net.Conn, queue chan *pb.DBRequest) {
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
		if err != nil {
			fmt.Println("Error reading from client " + err.Error())
			break
		}
		if uint32(count) != msgSize {
			go send(pb.DBReply_InvalidRequest, conn, errorChannel)
			continue
		}
		request := &pb.DBRequest{}
		if err := proto.Unmarshal(buffer, request); err != nil {
			go send(pb.DBReply_InvalidRequest, conn, errorChannel)
			continue
		}
		queue <- request
	}
	conn.Close()
}

func send(status pb.DBReply_Status, conn net.Conn, errorChannel chan bool) {
	var reply = new(pb.DBReply)
	reply.Status = status
	var buff, err = proto.Marshal(reply)
	if err != nil {
		fmt.Println("Cant marshal reply message")
		panic(1)
	}
	var size uint32 = uint32(len(buff))
	binary.Write(conn, binary.LittleEndian, size)
	count, err := conn.Write(buff)
	if err != nil || count != len(buff) {
		fmt.Println("Error writing to client" + err.Error())
		errorChannel <- false
	}
}
