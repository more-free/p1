package lsp

import (
	"container/list"
	"encoding/json"
	"fmt"
)

const (
	MAX_MSG_BYTES = 1024
)

func (m *Message) ToBytes() ([]byte, error) {
	return json.Marshal(m)
}

func IsAck(m *Message) bool {
	return m != nil && m.Type == MsgAck
}

func IsData(m *Message) bool {
	return m != nil && m.Type == MsgData
}

func IsConn(m *Message) bool {
	return m != nil && m.Type == MsgConnect
}

func IsConnAck(m *Message) bool {
	return m != nil && m.Type == MsgAck && m.ConnID > 0 && m.SeqNum == 0
}

func FromBytes(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return &msg, err
}

func GetAck(msg *Message) *Message {
	return &Message{
		Type:   MsgAck,
		ConnID: msg.ConnID,
		SeqNum: msg.SeqNum,
	}
}

func GetConnectionAck(connID int) *Message {
	return &Message{
		Type:   MsgAck,
		ConnID: connID,
		SeqNum: 0,
	}
}

func GetConnRequest() *Message {
	return &Message{
		Type:   MsgConnect,
		ConnID: 0,
		SeqNum: 0,
	}
}

func GetMessage(connID int, seqNum int, payload []byte) *Message {
	return &Message{
		Type:    MsgData,
		ConnID:  connID,
		SeqNum:  seqNum,
		Payload: payload,
	}
}

func EmptyMsg() []byte {
	return make([]byte, MAX_MSG_BYTES)
}

func EmptyAck(connId int) *Message {
	return &Message{
		Type:   MsgAck,
		ConnID: connId,
		SeqNum: 0,
	}
}

func EmptyPayload() []byte {
	return make([]byte, 0)
}

func WrapperMsg(payload []byte, connId int) *Message {
	return &Message{
		Type:    MsgData,
		ConnID:  connId,
		SeqNum:  -1,
		Payload: payload,
	}
}

// for wrapping payload only
func Msg(payload []byte) *Message {
	return &Message{
		Type:    MsgData,
		ConnID:  -1,
		SeqNum:  -1,
		Payload: payload,
	}
}

func CheckError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}

type MsgPair struct {
	msg *Message
	err error
}

type blockingQueue interface {
	Push(m *Message) error  // non-blocking.  return an error if it's closed
	Error(e error) error    // non-blocking.  explicitly push error into the queue
	Pop() (*Message, error) // block. get error if it is closed while waiting for incoming messages
	Close()                 // may cause error for push and pop
}

type unboundedBlockingQueue struct {
	queue *list.List
	push  chan *MsgPair
	req   chan struct{}
	res   chan *MsgPair
	quit  chan struct{}
}

func newUnboundedBlockingQueue() blockingQueue {
	q := &unboundedBlockingQueue{
		queue: list.New(),
		push:  make(chan *MsgPair),
		req:   make(chan struct{}),
		res:   make(chan *MsgPair),
		quit:  make(chan struct{}),
	}

	go q.start()
	return q
}

func (q *unboundedBlockingQueue) start() {
	request := 0
	for {
		select {
		case <-q.quit:
			return
		case msg := <-q.push:
			q.queue.PushBack(msg)
			if request > 0 {
				q.res <- q.popFirst()
				request--
			}
		case <-q.req:
			if q.queue.Len() > 0 {
				q.res <- q.popFirst()
			} else {
				request++
			}
		}
	}
}

func (q *unboundedBlockingQueue) popFirst() *MsgPair {
	front := q.queue.Front()
	q.queue.Remove(front)
	return front.Value.(*MsgPair)
}

func (q *unboundedBlockingQueue) Push(m *Message) error {
	select {
	case <-q.quit:
		return fmt.Errorf("blocking queue was closed")
	case q.push <- &MsgPair{m, nil}:
		return nil
	}
}

func (q *unboundedBlockingQueue) Pop() (*Message, error) {
	err := func() (*Message, error) {
		return nil, fmt.Errorf("blocking queue was closed")
	}

	select {
	case <-q.quit:
		return err()
	case q.req <- struct{}{}:
		select {
		case <-q.quit:
			return err()
		case m := <-q.res:
			return m.msg, m.err
		}
	}
}

func (q *unboundedBlockingQueue) Error(e error) error {
	select {
	case <-q.quit:
		return fmt.Errorf("blocking queue was closed")
	case q.push <- &MsgPair{nil, e}:
		return nil
	}
}

func (q *unboundedBlockingQueue) Close() {
	close(q.quit)
}
