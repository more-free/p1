// Contains the implementation of a LSP server.
package lsp

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type clientInfo struct {
	addr              *net.UDPAddr
	window            *Window
	nextReadSeq       int
	nextWriteSeq      int
	inMsgChan         chan *Message
	outMsgChan        chan *Message
	connActivityChan  chan bool
	closed            bool
	quitInMsgHandler  chan bool
	quitOutMsgHandler chan bool
	quitEpochHandler  chan bool
	outMsgQuit        chan bool
}

func (ci *clientInfo) keepConnAlive() {
	ci.connActivityChan <- true
}

type server struct {
	conn       map[int]*clientInfo // key = connID
	mutex      *sync.Mutex
	serverConn *net.UDPConn
	port       int
	params     *Params
	quit       bool
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	server := &server{
		conn:   make(map[int]*clientInfo),
		mutex:  &sync.Mutex{},
		port:   port,
		params: params,
		quit:   bool,
	}

	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	server.serverConn = conn

	go server.listen()

	return server, nil
}

func (s *server) newClientInfo(addr *net.UDPAddr) *clientInfo {
	return &clientInfo{
		addr:              addr,
		window:            NewWindow(s.params.WindowSize),
		nextReadSeq:       0,
		nextWriteSeq:      0,
		quitChan:          make(chan bool),
		inMsgChan:         make(chan *Message, MAX_WRITE_BUFFER),
		outMsgChan:        make(chan *Message, MAX_WRITE_BUFFER),
		connActivityChan:  make(chan bool),
		outMsgQuit:        make(chan bool),
		closed:            false,
		quitInMsgHandler:  make(chan bool),
		quitOutMsgHandler: make(chan bool),
		quitEpochHandler:  make(chan bool),
	}
}

// behave like a controller
func (s *server) listen() {
	data := make([]byte, MAX_MSG_SIZE)
	connID := 1

	isConnBuilt := func(msg *Message) bool {
		if msg.ConnID <= 0 {
			return false
		}

		_, exists := s.conn[msg.ConnID]
		return exists
	}

	for !s.quit {
		sz, addr, err := s.serverConn.ReadFromUDP(data)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		fmt.Println("connection request from client", addr)
		msg, err := FromBytes(data[:sz])
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		if msg != nil && msg.Type == MsgConnect {
			_, err = s.writeConnectionAck(connID, addr)
			if err != nil {
				fmt.Println(err.Error())
			} else {
				s.mutex.Lock()
				s.conn[connID] = s.newClientInfo(addr)
				s.mutex.Unlock()
				fmt.Println("connection established : ID =", connID)
				go s.handleInMsg(connID)
				go s.handleOutMsg(connID)
				go s.handleEpochEvents(connID)
				connID++
			}
		} else {
			if !isConnBuilt(msg) {
				fmt.Println("invalid message. connection is not established or was closed", msg)
			} else {
				ci, _ := s.conn[msg.ConnID]
				select {
				case ci.inMsgChan <- msg:
				default:
					fmt.Println(msg.ConnID, "in-buffer is full")
				}
			}
		}
	}
}

func (s *server) handleInMsg(connID int) {
	ci, _ := s.conn[connID]

	for {
		select {
		case msg := <-ci.inMsgChan:
			// ignore non-ack message if the connection is going to close
			if ci.closed && msg.Type != MsgAck {
				break
			}

			fmt.Println("receiving message on server : ", msg)
			ci.keepConnAlive()

			switch msg.Type {
			case MsgData:
				// we can ignore potential error due to epoch event handler
				ci.outMsgChan <- GetAck(msg)
				ci.window.SendAck(msg)
			case MsgAck:
				ci.window.ReceiveAck(msg)
			}

		// no incoming message anymore as connID should be removed
		// from server.conn, so any incoming message would be blocked by
		// listen()
		case <-ci.quitInMsgHandler:
			fmt.Println("exit in-message handler for connID", connID)
			return
		}
	}
}

func (s *server) handleOutMsg(connID int) {
	ci, _ := s.conn[connID]
	toQuit := false

Loop:
	for {
		select {
		case msg := <-ci.outMsgChan:
			switch msg.Type {
			case MsgAck:
				err := s.writeData(msg)
				CheckError(err)
			case MsgData:
				err := s.writeData(msg)
				ci.window.SendMsg(msg)
				CheckError(err)
			}

			if len(ci.outMsgChan) == 0 && toQuit {
				break Loop
			}

		// wait until all outgoing messages are handled
		// connID should not be removed before outgoing messages are all processed,
		// otherwise no further ack would be received
		case <-ci.quitOutMsgHandler:
			toQuit = true
			ci.closed = true // block any server.Write()

			if len(ci.outMsgChan) == 0 && toQuit {
				break Loop
			}
		}
	}

	ci.outMsgQuit <- true
}

func (s *server) handleEpochEvents(connID int) {
	ci, _ := s.conn[connID]
	retry := 0

	for {
		select {
		case <-time.After(time.Duration(s.params.EpochMillis) * time.Millisecond):
			retry++
			if retry >= s.params.EpochLimit {
				fmt.Println("quit due to too many inactive epoch")
				return
			}
			go s.resend(connID)

		case <-ci.connActivityChan:
			retry = 0

		case <-ci.quitEpochHandler:
			return
		}
	}
}

func (s *server) writeConnectionAck(connID int, addr *net.UDPAddr) (int, error) {
	msg := GetConnectionAck(connID)
	bytes, err := msg.ToBytes()
	if err != nil {
		return 0, err
	}

	sz, err := s.serverConn.WriteToUDP(bytes, addr)
	return sz, err
}

func (s *server) writeData(msg *Message) error {
	client, exists := s.conn[msg.ConnID]
	if !exists {
		return fmt.Errorf("client addr doesn't exist for ID = %v", msg.ConnID)
	}

	bytes, _ := msg.ToBytes()
	_, err := s.serverConn.WriteToUDP(bytes, client.addr)
	return err
}

func (s *server) resend(connID int) {
	ci, exists := s.conn[connID]
	if !exists {
		return
	}

	for _, msg := range ci.window.GetMsgForResend() {
		select {
		case ci.outMsgChan <- msg:
		default:
		}
	}

	for _, ack := range ci.window.GetAckForResend() {
		select {
		case ci.outMsgChan <- ack:
		default:
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	for connID, ci := range s.conn {
		ci.nextReadSeq++
		if msg := ci.window.getMessage(ci.nextReadSeq); msg != nil {
			return connID, msg.Payload, nil
		}
	}

	inChan := make(chan *Message)
	for connID, ci := range s.conn {

	}
}

func (s *server) Write(connID int, payload []byte) error {
	ci, exists := s.conn[connID]
	if !exists {
		return fmt.Errorf("connID %v does not exist", connID)
	}
	if ci.closed {
		return fmt.Errorf("connection %v already closed", connID)
	}

	ci.nextWriteSeq++
	msg := GetMessage(connID, ci.nextWriteSeq, payload)

	select {
	case s.outMsgChan <- msg:
	default:
		ci.nextWriteSeq--
		return fmt.Errorf("server write buffer is full")
	}

	return nil
}

func (s *server) CloseConn(connID int) error {
	return s.closeConn(connID, nil)
}

func (s *server) closeConn(connID int, done chan bool) error {
	ci, exists := s.conn[connID]
	if !exists {
		return fmt.Errorf("conn %v does not exist", connID)
	}

	go func() {
		// must close outMsgHandler first
		ci.closed = true // TODO race condition
		ci.quitOutMsgHandler <- true
		<-ci.outMsgQuit
		ci.quitEpochHandler <- true
		ci.quitInMsgHandler <- true

		if done != nil {
			done <- true
		}
	}()

	return nil
}

func (s *server) Close() error {
	s.serverConn.Close()

	for connID, _ := range s.conn {
		done := make(chan bool)
		s.closeConn(done)
		<-done
	}

	return nil
}
