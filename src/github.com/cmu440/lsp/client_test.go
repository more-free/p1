// run test :
// go test -v  client_test.go message.go util.go client_api.go client_impl.go params.go  window.go  server_api.go
package lsp

import (
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"
)

// a server echoing ack only. for test.
type EchoServer struct {
	conn       *net.UDPConn
	clientAddr *net.UDPAddr
	seqNum     int
	quit       bool
}

func NewEchoServer(port int) *EchoServer {
	addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf(":%v", port))
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Errorf(err.Error())
		return nil
	}

	fmt.Printf("udp server is listening on port %v\n", port)

	server := &EchoServer{
		conn:   conn,
		seqNum: 0,
		quit:   false,
	}

	go func() {
		for !server.quit {
			server.Read()
		}
	}()

	return server
}

func (s *EchoServer) Read() (int, []byte, error) {
	data := make([]byte, MAX_MSG_SIZE)
	sz, addr, err := s.conn.ReadFromUDP(data)
	if err != nil {
		fmt.Println("client closed connection. quit.")
		return 0, make([]byte, 0), err
	}
	s.clientAddr = addr

	msg, err := FromBytes(data[0:sz])
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("receiving message on server", msg, "from client", addr)
	}

	if msg.Type == MsgConnect {
		bytes, err := GetConnectionAck(1).ToBytes()
		if err != nil {
			fmt.Errorf(err.Error())
		}

		_, err = s.conn.WriteToUDP(bytes, addr)
		if err != nil {
			fmt.Println("error on writing to server", err.Error())
		}

	} else {
		fmt.Println("write ack for ", msg)
		bytes, _ := GetAck(msg).ToBytes()
		s.conn.WriteToUDP(bytes, addr)
	}

	return 0, data, nil
}

func (s *EchoServer) Write(connID int, payload []byte) error {
	s.seqNum++
	msg := GetMessage(connID, s.seqNum, payload)
	bytes, err := msg.ToBytes()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Println("writing message", msg, "to client", s.clientAddr)
	_, err = s.conn.WriteToUDP(bytes, s.clientAddr)
	return err
}

// for test only.
func (s *EchoServer) WriteWithSeqNum(connID int, seqNum int, payload []byte) error {
	msg := GetMessage(connID, seqNum, payload)
	bytes, err := msg.ToBytes()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Println("writing message", msg, "to client", s.clientAddr)
	_, err = s.conn.WriteToUDP(bytes, s.clientAddr)
	return err
}

func (s *EchoServer) CloseConn(connID int) error {
	s.quit = true
	return s.conn.Close()
}

func (s *EchoServer) Close() error {
	s.quit = true
	return s.conn.Close()
}

func TestBasicUDPMessage(t *testing.T) {
	runtime.GOMAXPROCS(2)

	server := NewEchoServer(8888)
	addr, _ := net.ResolveUDPAddr("udp4", ":8888")
	conn, _ := net.DialUDP("udp", nil, addr)

	bytes := make([]byte, MAX_MSG_SIZE)

	// build connection
	msg := GetConnectionAck(3)
	out, _ := msg.ToBytes()
	assertE(t, 47, len(out))

	conn.Write(out)
	sz, _ := conn.Read(bytes)
	msg, _ = FromBytes(bytes[:sz])
	assertE(t, MsgConnect, msg.Type)
	assertE(t, true, msg.ConnID > 0)
	assertE(t, 0, msg.SeqNum)
	connID := msg.ConnID

	// write / read data
	msg = GetMessage(connID, 5, []byte("hello"))
	out, _ = msg.ToBytes()
	assertE(t, 53, len(out))
	conn.Write(out)
	sz, _ = conn.Read(bytes)
	msg, _ = FromBytes(bytes[:sz])
	assertE(t, 5, msg.SeqNum)
	assertE(t, MsgAck, msg.Type)
	assertE(t, 1, msg.ConnID)

	// read data from server (server sends data actively)
	go server.Write(connID, []byte("world"))
	sz, _ = conn.Read(bytes)
	msg, _ = FromBytes(bytes[:sz])
	assertE(t, MsgData, msg.Type)
	assertE(t, 1, msg.SeqNum)
	assertE(t, connID, msg.ConnID)
	assertE(t, "world", string(msg.Payload))

	go server.Write(connID, []byte("world2"))
	sz, _ = conn.Read(bytes)
	msg, _ = FromBytes(bytes[:sz])
	assertE(t, MsgData, msg.Type)
	assertE(t, 2, msg.SeqNum)
	assertE(t, connID, msg.ConnID)
	assertE(t, "world2", string(msg.Payload))

	conn.Close()
	server.Close()
}

func TestClientWithEchoServer(t *testing.T) {
	runtime.GOMAXPROCS(2)

	server := NewEchoServer(8888)

	params := NewParams()
	params.WindowSize = 2
	params.EpochLimit = 5
	params.EpochMillis = 1000
	client, err := NewClient(":8888", params)

	defer client.Close()
	defer server.Close()

	assertE(t, nil, err)

	// build connection
	time.Sleep(time.Second * 1)
	assertE(t, 1, client.ConnID())

	// client -> server
	s := "hello"
	err = client.Write([]byte(s))
	assertE(t, nil, err)
	err = client.Write([]byte(s))
	assertE(t, nil, err)

	// server -> client
	time.Sleep(time.Second * 1)
	err = server.Write(client.ConnID(), []byte("world"))
	assertE(t, nil, err)
	err = server.Write(client.ConnID(), []byte("world2"))
	assertE(t, nil, err)

	bytes, err := client.Read()
	assertE(t, nil, err)
	assertE(t, "world", string(bytes))

	bytes, err = client.Read()
	assertE(t, nil, err)
	assertE(t, "world2", string(bytes))

	// server -> client. out-of-order delivery
	err = server.WriteWithSeqNum(client.ConnID(), 4, []byte("world4"))
	assertE(t, nil, err)
	err = server.WriteWithSeqNum(client.ConnID(), 3, []byte("world3"))
	assertE(t, nil, err)

	bytes, err = client.Read()
	assertE(t, nil, err)
	assertE(t, "world3", string(bytes))

	bytes, err = client.Read()
	assertE(t, nil, err)
	assertE(t, "world4", string(bytes))

	time.Sleep(time.Second * 1)
}

func TestClientAutoCloseAfterEpochLimit(t *testing.T) {
	runtime.GOMAXPROCS(2)

	server := NewEchoServer(8888)

	params := NewParams()
	params.EpochLimit = 2
	params.EpochMillis = 1000
	client, err := NewClient(":8888", params)
	assertE(t, nil, err)

	_, err = client.Read()
	assertE(t, "No data received within given epochs", err.Error())

	defer client.Close()
	defer server.Close()
}

func assertE(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("expected = %v, actual = %v", expected, actual)
	}
}

func assertF(t *testing.T) func(expected, actual interface{}) {
	return func(expected, actual interface{}) {
		if expected != actual {
			t.Errorf("expected = %v, actual = %v", expected, actual)
		}
	}
}

func bytesToInt(bytes []byte) int {
	var v int
	json.Unmarshal(bytes, &v)
	return v
}
