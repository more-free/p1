// Contains the implementation of a LSP client.

package lsp

import (
	"fmt"
	"time"
)

// implement MsgReadWriter
type clientMsgAdaptor struct {
	client UDPClient
	connId int
}

func (c *clientMsgAdaptor) Read() (*Message, error) {
	bytes := EmptyMsg()
	sz, err := c.client.Read(bytes)
	if err != nil {
		return nil, err
	}

	msg, err := FromBytes(bytes[:sz])
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (c *clientMsgAdaptor) Write(msg *Message) error {
	bytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	sz, err := c.client.Write(bytes)
	if err != nil {
		return err
	}

	if sz < len(bytes) {
		return fmt.Errorf("wrote size %v is less than full message size %v", sz, len(bytes))
	}

	return nil
}

func (c *clientMsgAdaptor) ConnID() int {
	return c.connId
}

func (c *clientMsgAdaptor) Close() error {
	return c.client.Close()
}

type client struct {
	lspRunner *LSPRunner
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	udpClient, err := NewFixedSizeUDPClient(MAX_MSG_BYTES, hostport)
	if err != nil {
		return nil, err
	}

	connId, err := initConn(udpClient, params)
	if err != nil {
		return nil, err
	}

	rw := &clientMsgAdaptor{
		client: udpClient,
		connId: connId,
	}
	lspRunner := NewLSPRunner(rw, params)
	lspRunner.Start() // non-blocking

	return &client{
		lspRunner: lspRunner,
	}, nil
}

func initConn(c UDPClient, params *Params) (int, error) {
	req, _ := GetConnRequest().ToBytes()
	res := EmptyMsg()

	quit := make(chan struct{})

	// keep sending connection request until conn ack is received
	go func() {
		for {
			select {
			case <-time.After(time.Duration(params.EpochMillis) * time.Millisecond):
				_, err := c.Write(req)
				if err != nil {
					return
				}
			case <-quit:
				return
			}
		}
	}()

	defer close(quit)
	for {
		sz, err := c.Read(res)
		if err != nil {
			return -1, err
		}

		msg, err := FromBytes(res[:sz])
		if err == nil && IsConnAck(msg) {
			return msg.ConnID, nil
		}
	}
}

func (c *client) ConnID() int {
	return c.lspRunner.rw.ConnID()
}

func (c *client) Read() ([]byte, error) {
	return c.lspRunner.Read()
}

func (c *client) Write(payload []byte) error {
	return c.lspRunner.Write(payload)
}

func (c *client) Close() error {
	return c.lspRunner.Stop()
}
