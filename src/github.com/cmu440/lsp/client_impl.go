// Contains the implementation of a LSP client.

package lsp

import (
	"fmt"
	net "github.com/cmu440/lspnet"
	"time"
)

type client struct {
	hostport      string
	params        *Params
	connId        int
	conn          *net.UDPConn
	window        *Window
	pending       []*Message
	nextReadSeq   int
	nextWriteSeq  int
	readMsgChan   chan *Message
	writeMsgChan  chan *Message
	errChan       chan error
	quitWriteChan chan bool
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
	client := &client{
		hostport:      hostport,
		params:        params,
		connId:        -1,
		window:        NewWindow(params.WindowSize),
		pending:       make([]*Message, 0),
		nextReadSeq:   0,
		nextWriteSeq:  0,
		readMsgChan:   make(chan *Message, params.WindowSize), // size is optional
		writeMsgChan:  make(chan *Message, MAX_WRITE_BUFFER),  // size is must
		errChan:       make(chan error, 2),                    // must have buffer >= 2 to tolerate errors that have no receiver
		quitWriteChan: make(chan bool),
	}

	err := client.buildConn() // block until connection created or timeout
	if err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

func (c *client) buildConn() error {
	serverAddr, _ := net.ResolveUDPAddr("udp4", c.hostport)
	conn, err := net.DialUDP("udp", nil, serverAddr)

	if err != nil {
		return err
	}
	c.conn = conn
	go c.readFromServer()
	go c.writeToServer()

	initMsg := &Message{
		Type:   MsgConnect,
		ConnID: 0,
		SeqNum: 0,
	}

	var isConnBuilt = func(m *Message) bool {
		return m != nil && m.SeqNum == 0 && m.Type == MsgConnect
	}

	for retry := 0; retry < c.params.EpochLimit; retry++ {
		c.writeMsgChan <- initMsg
		select {
		case msg := <-c.readMsgChan:
			if isConnBuilt(msg) {
				c.connId = msg.ConnID
				return nil
			}
		case <-time.After(time.Duration(c.params.EpochMillis) * time.Millisecond):
			continue
		}
	}

	c.Close()
	return fmt.Errorf("Could not establish connection with %v within %v epoch",
		c.hostport, c.params.EpochLimit)
}

func (c *client) readFromServer() {
	data := make([]byte, MAX_MSG_SIZE)

	for {
		sz, err := c.conn.Read(data)
		if err != nil { // conn is closed by either client or server
			fmt.Errorf("conn error : %v", err.Error())
			break
		}

		msg, err := FromBytes(data[:sz])
		if err != nil {
			fmt.Errorf(err.Error())
		} else {
			c.readMsgChan <- msg
		}
	}
}

func (c *client) writeToServer() {
	toQuit := false

	for {
		select {
		case msg := <-c.writeMsgChan:
			bytes, _ := msg.ToBytes()
			_, err := c.conn.Write(bytes)
			if err != nil {
				fmt.Errorf(err.Error())
			}
			if len(c.writeMsgChan) == 0 && toQuit {
				return
			}

		case <-c.quitWriteChan:
			if len(c.writeMsgChan) != 0 {
				toQuit = true
			} else {
				return
			}
		}
	}
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {
	msg, err := c.readMsg()
	if err != nil {
		return make([]byte, 0), err
	} else {
		return msg.Payload, nil
	}
}

func (c *client) readMsg() (*Message, error) {
	c.nextReadSeq++

	// if the expected message has already been received, return it
	if msg := c.window.getMessage(c.nextReadSeq); msg != nil {
		return msg, nil
	}

	retry := 0
	for {
		select {
		case <-time.After(time.Duration(c.params.EpochMillis) * time.Millisecond):
			if c.window.hasWaitingMessage() {
				go c.resend()
				break
			}

			retry++
			if retry > c.params.EpochLimit {
				c.Close()
				return nil, fmt.Errorf("No data received within given epochs")
			} else {
				go c.resend()
			}

		case msg := <-c.readMsgChan:
			if msg.Type == MsgAck {
				c.window.ReceiveAck(msg)
			} else {
				fmt.Println("client receives msg", msg)
				c.window.SendAck(msg)
				ack := GetAck(msg)
				c.writeMsgChan <- ack
				if msg.SeqNum == c.nextReadSeq {
					return msg, nil
				}
			}
		}
	}
}

func (c *client) resend() {
	// if writeMsgChan is full, ignore it, and leave it to the next epoch
	// this may also prevent the deadlock : client is closed before resend() finishes
	// (so c.writeMsgChan <- msg) blocks indefinitely
	for _, msg := range c.window.GetMsgForResend() {
		select {
		case c.writeMsgChan <- msg:
		default:
		}
	}

	for _, ack := range c.window.GetAckForResend() {
		select {
		case c.writeMsgChan <- ack:
		default:
		}
	}

	// resend all pending messages
	newPending := make([]*Message, 0)
	for _, msg := range c.pending {
		err := c.window.SendMsg(msg)
		if err == nil {
			c.writeMsgChan <- msg
		} else {
			newPending = append(newPending, msg)
		}
	}

	c.pending = newPending
}

func (c *client) Write(payload []byte) error {
	if c.connId < 0 {
		return fmt.Errorf("Connection not established")
	}

	c.nextWriteSeq++

	msg := &Message{
		Type:    MsgData,
		ConnID:  c.ConnID(),
		SeqNum:  c.nextWriteSeq,
		Payload: payload,
	}

	// non-blocking until MAX_WRITE_BUFFER is reached
	err := c.window.SendMsg(msg)
	if err == nil {
		c.writeMsgChan <- msg
	} else {
		exists := false
		for _, m := range c.pending {
			if m.SeqNum == msg.SeqNum {
				exists = true
				break
			}
		}
		if !exists {
			c.pending = append(c.pending, msg) // wait for next resend
		}
	}

	return nil
}

func (c *client) Close() error {
	if c.connId < 0 {
		return nil
	}

	c.conn.Close()          // exit go routine readFromServer
	c.quitWriteChan <- true // exit go routine writeToServer
	c.connId = -1
	return nil
}
