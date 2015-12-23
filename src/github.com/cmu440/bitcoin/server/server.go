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

func NewEventHandler(s *Handler) impl.EventHandler {
	return func(event *impl.Event) {
		switch event.Type {
		case impl.ClientRequest:
			s.HandleClientRequest(event.ID, event.Msg)
		case impl.WorkerJoin:
			s.HandleWorkerJoin(event.ID)
		case impl.WorkerDone:
			s.HandleWorkerDone(event.ID, event.Msg)
		case impl.ConnectionLost:
			s.HandleConnectionLost(event.ID, event.Err)
		}
	}
}

type Handler struct {
	workerProxy impl.WorkerProxy
	*impl.IDManager
}

func NewHandler(taskToStart chan<- *impl.TaskAssign, taskResult chan<- *impl.TaskResult) *Handler {
	mapper := impl.NewFairScheduler()
	reducer := impl.NewReducer()
	return &Handler{
		impl.NewOneForOneWorkerProxy(mapper, reducer, taskToStart, taskResult),
		impl.NewIDManager(),
	}
}

func (s *Handler) HandleClientRequest(clientConnID int, req *bitcoin.Message) {
	requestID := s.GetUniqueRequestID()
	s.AddClientIfAnsent(clientConnID)
	s.SetRequestToClient(requestID, clientConnID)

	task := impl.NewTask(impl.RequestID(requestID), req)
	s.workerProxy.Schedule(task)
}

func (s *Handler) HandleWorkerJoin(workerConnID int) {
	s.AddWorkerIfAbsent(workerConnID)
	s.workerProxy.HandleWorkerJoin(impl.WorkerID(workerConnID))
}

func (s *Handler) HandleWorkerDone(workerConnID int, res *bitcoin.Message) {
	partialResult := impl.NewTaskPartialResult(impl.WorkerID(workerConnID), res)
	s.workerProxy.HandlePartialResult(partialResult)
}

func (s *Handler) HandleConnectionLost(connID int, cause error) {
	if s.IsClient(connID) {
		s.RemoveClient(connID)
	} else { // workerID
		s.RemoveWorker(connID)
		s.workerProxy.HandleWorkerLost(impl.WorkerID(connID))
	}
}

type Server struct {
	networkManager impl.NetworkManager
	handler        *Handler
	taskStartChan  chan *impl.TaskAssign
	taskResultChan chan *impl.TaskResult
	quit           chan struct{}
}

func NewServer(port int, params *lsp.Params) (*Server, error) {
	taskStartChan := make(chan *impl.TaskAssign)
	taskResultChan := make(chan *impl.TaskResult)
	handler := NewHandler(taskStartChan, taskResultChan)
	networkManager, err := impl.NewNetworkManager(port, params, NewEventHandler(handler), NewEventParser())
	if err != nil {
		return nil, err
	}

	server := &Server{
		networkManager: networkManager,
		handler:        handler,
		taskStartChan:  taskStartChan,
		taskResultChan: taskResultChan,
		quit:           make(chan struct{}),
	}

	return server, nil
}

func (s *Server) Start() {
	for {
		select {
		case taskAssign := <-s.taskStartChan:
			s.write(int(taskAssign.ID), taskAssign.Task.Request)

		case taskResult := <-s.taskResultChan:
			client, err := s.handler.GetClientByRequest(int(taskResult.ID))
			// non-nil error indicates client connection lost, in that case we ignore the result
			if err == nil {
				s.write(client, taskResult.Result)
			}

		case <-s.quit:
			close(s.taskStartChan)
			close(s.taskResultChan)
			return
		}
	}
}

func (s *Server) write(connID int, msg *bitcoin.Message) {
	err := s.networkManager.Send(connID, msg)
	if err != nil {
		log.Printf("failed to send message to %v : %v", connID, err)
		s.handler.HandleConnectionLost(connID, err)
	}
}

func (s *Server) Stop() {
	close(s.quit)
	s.networkManager.Close()
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
