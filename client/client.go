package client

import (
	"encoding/binary"
	"net"

	"github.com/danch/danchbase/pb"
	"github.com/golang/protobuf/proto"
)

const ProtocolVersion = 1

//Client represents a danchbase client connection
type Client struct {
	connection net.Conn
}

type ClientError struct {
	msg    string
	status *pb.DBReply_Status
}

func (err *ClientError) Error() string {
	return err.msg
}
func newClientError(msg string, status *pb.DBReply_Status) *ClientError {
	err := new(ClientError)
	err.msg = msg
	err.status = status
	return err
}

//NewClient creates connects to the indicated host and port and returns a
//Client object
func NewClient(hostAndPort string) (*Client, error) {
	conn, err := net.Dial("tcp", hostAndPort)
	if err != nil {
		return nil, err
	}
	var client = new(Client)
	client.connection = conn
	return client, nil
}

// Close the connection
func (client *Client) Close() {
	client.connection.Close()
}

// Put a record to the server
func (client *Client) Put(key string, value []byte) error {
	request := new(pb.DBRequest)
	request.Version = ProtocolVersion
	request.Verb = pb.DBRequest_Put
	request.DbName = "notimplemented"
	request.TableName = "notimplemented"
	request.StartKey = key
	request.Datalength = int32(len(value))

	buffer, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	var msgSize uint32 = uint32(len(buffer))
	err = binary.Write(client.connection, binary.LittleEndian, msgSize)
	if err != nil {
		return err
	}
	count, err := client.connection.Write(buffer)
	if count != len(buffer) {
		return newClientError("Error sending request (too few bytes written)", nil)
	}
	if err != nil {
		return err
	}
	count, err = client.connection.Write(value)
	if count != len(value) {
		return newClientError("Error sending request (too few bytes written)", nil)
	}
	if err != nil {
		return err
	}

	//TODO asycn send
	err = binary.Read(client.connection, binary.LittleEndian, &msgSize)
	buffer = make([]byte, msgSize)
	count, err = client.connection.Read(buffer)
	if count != len(value) {
		return newClientError("Error sending request (too few bytes read)", nil)
	}
	if err != nil {
		return err
	}
	var reply = new(pb.DBReply)
	err = proto.Unmarshal(buffer, reply)
	if reply.GetStatus() != pb.DBReply_Success {
		var stat = reply.GetStatus()
		return newClientError("Error from server", &stat)
	}

	return nil
}
