package server

import (
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
)

/*
a stateless web-server for sending/receiving message to/from clients/workers, and invoke event handler provides
by high-level caller. it doesn't store any data about the real clients/workers.
Note that there is no need to perform a heartbeat check for clients/workers, as lsp protocol already includes
a health check mechanism - at least one message will be sent by each direction per epoch.
*/
type NetworkManager interface {
	Send(id int, msg *bitcoin.Message) error
	Close() error
}

type networkManager struct {
	port    int
	params  *lsp.Params
	server  lsp.Server
	handler EventHandler
	parser  EventParser
	closed  bool
}

func NewNetworkManager(port int, params *lsp.Params, handler EventHandler, parser EventParser) (NetworkManager, error) {
	server, err := lsp.NewServer(port, params)
	if err != nil {
		return &networkManager{}, err
	}

	manager := &networkManager{
		port:    port,
		params:  params,
		server:  server,
		handler: handler,
		parser:  parser,
		closed:  false,
	}

	go manager.start()
	return manager, nil
}

func (w *networkManager) start() {
	for !w.closed { // race condition though not a big issue here
		id, payload, err := w.server.Read()
		if err != nil && id == 0 { // Close() invoked explicitly
			log.Println("quit worker proxy")
			return
		}

		event := w.parser(id, payload, err)
		w.handler(event)
	}
}

func (w *networkManager) Send(id int, msg *bitcoin.Message) error {
	bytes, _ := msg.ToBytes()
	return w.server.Write(id, bytes)
}

func (w *networkManager) Close() error {
	w.closed = true
	return w.server.Close()
}
