// Contains the implementation of a LSP server.
package lsp

import (
	"fmt"
	net "github.com/cmu440/lspnet"
	"log"
)

// implement MsgReadWriter, used by clientHandler
type serverMsgAdaptor struct {
	server     UDPServer // shared by multiple serverMgsAdaptor instances
	connId     int
	clientAddr *net.UDPAddr
	in         blockingQueue
}

func newServerMgsAdaptor(server UDPServer, connId int, clientAddr *net.UDPAddr, in blockingQueue) *serverMsgAdaptor {
	return &serverMsgAdaptor{
		server:     server,
		connId:     connId,
		clientAddr: clientAddr,
		in:         in,
	}
}

func (s *serverMsgAdaptor) Read() (*Message, error) {
	return s.in.Pop()
}

func (s *serverMsgAdaptor) Write(msg *Message) error {
	bytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	sz, err := s.server.Write(bytes, s.clientAddr)
	if err != nil {
		return err
	}

	if sz < len(bytes) {
		return fmt.Errorf("wrote size %v less than full message size %v", sz, len(bytes))
	}

	return nil
}

func (s *serverMsgAdaptor) ConnID() int {
	return s.connId
}

func (s *serverMsgAdaptor) Close() error {
	s.in.Close()
	return nil
}

type clientHandler struct {
	lspRunner *LSPRunner
	in        blockingQueue
}

func newClientHandler(rw MsgReadWriter, params *Params, in blockingQueue) *clientHandler {
	return &clientHandler{
		lspRunner: NewLSPRunner(LSPServer, rw, params),
		in:        in,
	}
}

func (c *clientHandler) Close() {
	c.in.Close()
	c.lspRunner.Stop()
}

type server struct {
	params     *Params              // shared by multiple clientHandler
	udpServer  UDPServer            // shared by multi clientHandler
	in         blockingQueue        // listening any incoming message
	quit       chan struct{}        // broadcast to multi clientHandler
	addrToId   map[string]int       // client addr -> connID
	idToAddr   map[int]*net.UDPAddr // connID -> client addr. reverse lookup
	nextConnId int
	handlers   map[int]*clientHandler // connID -> clientHandler
}

// helper type definitions
type inMsg struct {
	msg     *Message
	srcAddr *net.UDPAddr
	err     error
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	udpServer, err := NewFixedSizeUDPServer(MAX_MSG_BYTES, port)
	if err != nil {
		return nil, err
	}

	s := &server{
		params:     params,
		udpServer:  udpServer,
		in:         newUnboundedBlockingQueue(),
		quit:       make(chan struct{}),
		addrToId:   make(map[string]int),
		idToAddr:   make(map[int]*net.UDPAddr),
		nextConnId: 1,
		handlers:   make(map[int]*clientHandler),
	}

	go s.listen()
	return s, nil
}

func (s *server) listen() {
	in := make(chan *inMsg)
	go s.readMsg(in)

	for {
		select {
		case m := <-in:
			if m.err != nil {
				log.Printf("exiting listener due to err %v", m.err)
				return
			}

			// handlers should : either run in a go routine, or be intrinsically non-blocking.
			// here handleConn is blocking as as nextConnId is not thread safe
			switch m.msg.Type {
			case MsgConnect:
				s.handleConn(m)
			default:
				s.handleDataAck(m)
			}

		case <-s.quit:
			return
		}
	}
}

func (s *server) readMsg(out chan<- *inMsg) {
	data := make([]byte, s.udpServer.MaxMsgSize())
	for {
		sz, srcAddr, err := s.udpServer.Read(data)
		if err != nil { // the only cause : udpServer is closed explicitly
			out <- &inMsg{
				msg:     nil,
				srcAddr: srcAddr,
				err:     fmt.Errorf("server was closed explicitly : %v", err),
			}
			break
		}

		msg, err := FromBytes(data[:sz])
		if err != nil {
			log.Printf("received and ignored invalid message : %v", err)
			continue
		}

		out <- &inMsg{
			msg:     msg,
			srcAddr: srcAddr,
			err:     nil,
		}
	}
}

func (s *server) handleConn(conn *inMsg) {
	connId, exists := s.addrToId[conn.srcAddr.String()]
	if !exists {
		s.addrToId[conn.srcAddr.String()] = s.nextConnId
		in := newUnboundedBlockingQueue()
		rw := newServerMgsAdaptor(s.udpServer, s.nextConnId, conn.srcAddr, in)
		handler := newClientHandler(rw, s.params, in)
		s.idToAddr[s.nextConnId] = conn.srcAddr
		s.handlers[s.nextConnId] = handler

		// write conn ack
		bytes, _ := GetConnectionAck(s.nextConnId).ToBytes()
		_, err := s.udpServer.Write(bytes, conn.srcAddr)
		if err != nil {
			log.Printf("error while writing conn ack %v", err)
		}

		// start both epoch and reader in the handler
		s.startHandler(s.nextConnId, handler)

		s.nextConnId++
	} else {
		// write ack again.  during connection building stage, server will not resend ack.
		// if client doesn't receive ack for certain period, it resends the request again.
		handler := s.handlers[connId]
		bytes, _ := GetConnectionAck(connId).ToBytes()
		err := handler.lspRunner.Write(bytes)
		if err != nil {
			log.Printf("error while writing conn ack %v", err)
		}
	}
}

func (s *server) startHandler(connId int, handler *clientHandler) {
	handler.lspRunner.Start()

	go func() {
		for {
			payload, err := handler.lspRunner.Read()
			if err == nil {
				s.in.Push(WrapperMsg(payload, connId))
			} else {
				break
			}
		}
	}()
}

func (s *server) handleDataAck(dataAck *inMsg) {
	handler, exists := s.handlers[dataAck.msg.ConnID]
	if exists {
		handler.in.Push(dataAck.msg)
	}
}

func (s *server) Read() (int, []byte, error) {
	msg, err := s.in.Pop()
	if err != nil {
		return -1, EmptyPayload(), err
	} else {
		return msg.ConnID, msg.Payload, nil
	}
}

func (s *server) Write(connID int, payload []byte) error {
	handler, exists := s.handlers[connID]
	if !exists {
		return fmt.Errorf("connection id %v doesn't exist", connID)
	}
	return handler.lspRunner.Write(payload)
}

// TODO make it non-blocking
func (s *server) CloseConn(connID int) error {
	handler, exists := s.handlers[connID]
	if !exists {
		return fmt.Errorf("connection id %v doesn't exist", connID)
	}
	err := handler.lspRunner.Stop()
	addr := s.idToAddr[connID]
	delete(s.idToAddr, connID)
	delete(s.addrToId, addr.String())
	delete(s.handlers, connID)
	return err
}

// TODO make it blocking
func (s *server) Close() error {
	for connId, _ := range s.handlers {
		s.CloseConn(connId)
	}
	return s.udpServer.Close()
}
