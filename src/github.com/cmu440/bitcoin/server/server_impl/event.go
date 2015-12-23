package server

import "github.com/cmu440/bitcoin"

type EventType int

const (
	WorkerJoin EventType = iota
	WorkerDone
	ClientRequest
	ConnectionLost // either client or worker
	Nothing
)

type Event struct {
	Type EventType
	ID   int // source id who triggers the event
	Msg  *bitcoin.Message
	Err  error
}

func NewWorkerJoinEvent(id int) *Event {
	return &Event{
		Type: WorkerJoin,
		ID:   id,
	}
}

func NewWorkerDoneEvent(id int, result *bitcoin.Message) *Event {
	return &Event{
		Type: WorkerDone,
		ID:   id,
		Msg:  result,
	}
}

func NewConnLostEvent(id int, err error) *Event {
	return &Event{
		Type: ConnectionLost,
		ID:   id,
		Err:  err,
	}
}

func NewClientRequestEvent(id int, request *bitcoin.Message) *Event {
	return &Event{
		Type: ClientRequest,
		ID:   id,
		Msg:  request,
	}
}

func NewNilEvent() *Event {
	return &Event{
		Type: Nothing,
	}
}

type EventHandler func(event *Event)
type EventParser func(id int, payload []byte, err error) *Event
