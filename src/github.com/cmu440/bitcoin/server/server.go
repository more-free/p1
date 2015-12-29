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
	idManager        *impl.IDManager
	taskStartChan    chan *impl.TaskAssign
	taskResultChan   chan *impl.TaskResult
	networkEventChan chan *impl.Event
	quit             chan struct{}
}

func NewServer(port int, params *lsp.Params) (*Server, error) {
	taskStartChan := make(chan *impl.TaskAssign)
	taskResultChan := make(chan *impl.TaskResult)
	networkEventChan := make(chan *impl.Event)
	networkManager, err := impl.NewNetworkManager(port, params, NewEventParser(), networkEventChan)
	if err != nil {
		return nil, err
	}

	mapper := impl.NewFairScheduler()
	reducer := impl.NewReducer()
	workerProxy := impl.NewOneForOneWorkerProxy(mapper, reducer, taskStartChan, taskResultChan)
	idManager := impl.NewIDManager()

	return &Server{
		networkManager:   networkManager,
		workerProxy:      workerProxy,
		idManager:        idManager,
		taskStartChan:    taskStartChan,
		taskResultChan:   taskResultChan,
		networkEventChan: networkEventChan,
		quit:             make(chan struct{}),
	}, nil
}

func (s *Server) Start() {
	for {
		select {
		case taskAssign := <-s.taskStartChan:
			log.Printf("assign task %v to worker %v", taskAssign.Task.Request, taskAssign.ID)
			s.write(taskAssign.ID.ToInt(), taskAssign.Task.Request)

		case taskResult := <-s.taskResultChan:
			client, err := s.idManager.GetClientByRequest(taskResult.ID.ToInt())
			// non-nil error indicates a client connection lost, in which case we ignore the result
			if err == nil {
				s.write(client, taskResult.Result)
			}

		case event := <-s.networkEventChan:
			s.handleNetworkEvent(event)

		case <-s.quit:
			close(s.taskStartChan)
			close(s.taskResultChan)
			return
		}
	}
}

func (s *Server) Stop() {
	close(s.quit)
	s.networkManager.Close()
}

func (s *Server) handleNetworkEvent(event *impl.Event) {
	switch event.Type {
	case impl.ClientRequest:
		log.Printf("received client request %v from %v", event.Msg, event.ID)
		s.handleClientRequest(event.ID, event.Msg)
	case impl.WorkerJoin:
		log.Printf("received worker join %v", event.ID)
		s.handleWorkerJoin(event.ID)
	case impl.WorkerDone:
		log.Printf("worker job done %v", event.Msg)
		s.handleWorkerDone(event.ID, event.Msg)
	case impl.ConnectionLost:
		log.Printf("connection lost %v", event.ID)
		s.handleConnectionLost(event.ID, event.Err)
	}
}

func (s *Server) handleClientRequest(clientConnID int, req *bitcoin.Message) {
	requestID := s.idManager.GetUniqueRequestID()
	s.idManager.AddClientIfAnsent(clientConnID)
	s.idManager.SetRequestToClient(requestID, clientConnID)

	task := impl.NewTask(impl.ToRequestID(requestID), req)
	s.workerProxy.Schedule(task)
}

func (s *Server) handleWorkerJoin(workerConnID int) {
	s.idManager.AddWorkerIfAbsent(workerConnID)
	s.workerProxy.HandleWorkerJoin(impl.ToWorkerID(workerConnID))
}

func (s *Server) handleWorkerDone(workerConnID int, res *bitcoin.Message) {
	partialResult := impl.NewTaskPartialResult(impl.ToWorkerID(workerConnID), res)
	s.workerProxy.HandlePartialResult(partialResult)
}

func (s *Server) handleConnectionLost(connID int, cause error) {
	if s.idManager.IsClient(connID) {
		s.idManager.RemoveClient(connID)
	} else { // workerID
		s.idManager.RemoveWorker(connID)
		s.workerProxy.HandleWorkerLost(impl.ToWorkerID(connID))
	}
}

func (s *Server) write(connID int, msg *bitcoin.Message) {
	err := s.networkManager.Send(connID, msg)
	if err != nil {
		log.Printf("failed to send message to %v : %v", connID, err)
		s.handleConnectionLost(connID, err)
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
