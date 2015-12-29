package main

import (
	"fmt"
	"github.com/cmu440/bitcoin"
	impl "github.com/cmu440/bitcoin/server/server_impl"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

func NewEventParser() impl.EventParser {
	return func(id int, payload []byte, err error) *impl.Event {
		if err != nil {
			return impl.NewConnLostEvent(id, err)
		}

		msg, _ := bitcoin.FromBytes(payload)
		switch msg.Type {
		case bitcoin.Request:
			return impl.NewClientRequestEvent(id, msg)
		case bitcoin.Join:
			return impl.NewWorkerJoinEvent(id)
		case bitcoin.Result:
			return impl.NewWorkerDoneEvent(id, msg)
		default:
			return impl.NewNilEvent()
		}
	}
}

type Server struct {
	networkManager   impl.NetworkManager
	workerProxy      impl.WorkerProxy
	clients          *bitcoin.IntSet
	workers          *bitcoin.IntSet
	taskStartChan    chan *impl.TaskStart
	taskDoneChan     chan *impl.TaskDone
	networkEventChan chan *impl.Event
	quit             chan struct{}
}

func NewServer(port int, params *lsp.Params) (*Server, error) {
	taskStartChan := make(chan *impl.TaskStart)
	taskDoneChan := make(chan *impl.TaskDone)
	networkEventChan := make(chan *impl.Event)
	networkManager, err := impl.NewNetworkManager(port, params, NewEventParser(), networkEventChan)
	if err != nil {
		return nil, err
	}

	scheduler := impl.NewFairScheduler()
	workerProxy := impl.NewWorkerProxy(scheduler, taskStartChan, taskDoneChan)

	return &Server{
		networkManager:   networkManager,
		workerProxy:      workerProxy,
		clients:          bitcoin.NewIntSet(),
		workers:          bitcoin.NewIntSet(),
		taskStartChan:    taskStartChan,
		taskDoneChan:     taskDoneChan,
		networkEventChan: networkEventChan,
		quit:             make(chan struct{}),
	}, nil
}

func (s *Server) Start() {
	for {
		select {
		case taskStart := <-s.taskStartChan:
			log.Printf("assign task %v to worker %v", taskStart.Request, taskStart.WorkerID)
			s.write(taskStart.WorkerID, (*bitcoin.Message)(taskStart.Request))

		case taskDone := <-s.taskDoneChan:
			log.Printf("task from client %v was done : %v", taskDone.ClientID, taskDone.Result)
			s.write(taskDone.ClientID, (*bitcoin.Message)(taskDone.Result))

		case event := <-s.networkEventChan:
			s.handleNetworkEvent(event)

		case <-s.quit:
			close(s.taskStartChan)
			close(s.taskDoneChan)
			return
		}
	}
}

func (s *Server) Stop() {
	close(s.quit)
	s.networkManager.Close()
}

// all event handlers should be non-blocking
func (s *Server) handleNetworkEvent(event *impl.Event) {
	switch event.Type {
	case impl.ClientRequest:
		log.Printf("received client request %v from %v", event.Msg, event.ID)
		s.handleClientRequest(event.ID, event.Msg)
	case impl.WorkerJoin:
		log.Printf("received worker join %v", event.ID)
		s.handleWorkerJoin(event.ID)
	case impl.WorkerDone:
		log.Printf("worker %v finished subtask %v - %v", event.ID, event.Msg.Id, event.Msg)
		s.handleWorkerDone(event.ID, event.Msg)
	case impl.ConnectionLost:
		log.Printf("connection with %v lost due to error %v", event.ID, event.Err)
		s.handleConnectionLost(event.ID)
	}
}

func (s *Server) handleClientRequest(clientConnID int, req *bitcoin.Message) {
	s.clients.Add(clientConnID)
	s.workerProxy.HandleRequest(req, clientConnID)
}

func (s *Server) handleWorkerJoin(workerConnID int) {
	s.workers.Add(workerConnID)
	s.workerProxy.HandleWorkerJoin(workerConnID)
}

func (s *Server) handleWorkerDone(workerConnID int, res *bitcoin.Message) {
	s.workerProxy.HandleResult(impl.Result(res), workerConnID)
}

func (s *Server) handleConnectionLost(connID int) {
	if s.clients.Contains(connID) {
		s.clients.Remove(connID)
	} else if s.workers.Contains(connID) {
		s.workers.Remove(connID)
		s.workerProxy.HandleWorkerLost(connID)
	} else {
		// ignore non-existing ids
	}
}

func (s *Server) write(connID int, msg *bitcoin.Message) {
	err := s.networkManager.Send(connID, msg)
	if err != nil {
		log.Printf("failed to send message to client/worker %v due to error %v", connID, err)
		s.handleConnectionLost(connID)
	}
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("cannot parse port number ", err)
		return
	}

	params := lsp.NewParams()
	server, err := NewServer(port, params)
	if err != nil {
		fmt.Println("cannot start server", err)
		return
	}

	server.Start()
}
