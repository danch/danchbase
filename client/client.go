package client

import (
	"encoding/binary"
	"net"

	"github.com/danch/danchbase/pb"
	"github.com/golang/protobuf/proto"
)


//Client represents a danchbase client connection
type Client struct {
	connection net.Conn
}

//ClientError error carrying status
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
//Get a record from the database
func (client *Client) Get(key string) (*pb.DBReply, error) {
	request := pb.NewRequest(pb.DBRequest_Get, "notimplemented", "notimplemented", key, nil)
	return client.exchange(request)
}

// Put a record to the server
func (client *Client) Put(key string, value []byte) (*pb.DBReply, error) {
	request := pb.NewRequest(pb.DBRequest_Put, "notimplemented", "notimplemented", key, value)
	return client.exchange(request)
}

func (client *Client) exchange(request *pb.DBRequest) (*pb.DBReply, error) {
	err := client.sendMessage(request)
	if (err != nil) {
		return nil, err
	}

	//TODO asycn send
	return client.getReply()
}

func (client *Client) getReply() (*pb.DBReply, error) {
	var msgSize uint32
	err := binary.Read(client.connection, binary.LittleEndian, &msgSize)
	var buffer = make([]byte, msgSize)
	count, err := client.connection.Read(buffer)
	if count != int(msgSize) {
		return nil, newClientError("Error sending request (too few bytes read)", nil)
	}
	if err != nil {
		return nil, err
	}
	var reply = new(pb.DBReply)
	err = proto.Unmarshal(buffer, reply)
	if !pb.Success(reply.GetStatus()) {
		var stat = reply.GetStatus()
		return nil, newClientError("Error from server", &stat)
	}
	return reply, nil
}

func (client *Client) sendMessage(request *pb.DBRequest) error {
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
	return nil
}