package lsp

import (
	"container/list"
	"encoding/json"
	"fmt"
	"sync"
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

type blockingQueue interface {
	Push(m *Message)        // never block
	Error(e error)          // explicitly push an error to the queue
	Pop() (*Message, error) // block. get error if it is closed while waiting for incoming messages
	Top() (*Message, error) // non-blocking
	Close()                 // unblocking any Pop() with nil
}

// the simplest blocking queue : one consumer & one producer
type oneToOneBlockingQueue struct {
	msgs     *list.List
	arrival  chan struct{}
	producer *sync.Mutex
	consumer *sync.Mutex
}

func newOneToOneBlockingQueue() blockingQueue {
	return &oneToOneBlockingQueue{
		msgs:     list.New(),
		arrival:  make(chan struct{}, 1),
		producer: &sync.Mutex{},
		consumer: &sync.Mutex{},
	}
}

func (q *oneToOneBlockingQueue) Push(m *Message) {
	q.producer.Lock()
	defer q.producer.Unlock()

	q.msgs.PushBack(m)
	select {
	case q.arrival <- struct{}{}:
	default:
	}
}

func (q *oneToOneBlockingQueue) Error(e error) {

}

func (q *oneToOneBlockingQueue) Top() (*Message, error) {
	if q.msgs.Len() == 0 {
		return nil, fmt.Errorf("empty queue")
	} else {
		return q.msgs.Front().Value.(*Message), nil
	}
}

func (q *oneToOneBlockingQueue) Pop() (*Message, error) {
	q.consumer.Lock()
	defer q.consumer.Unlock()

	var top *Message
	for q.msgs.Len() <= 0 {
		_, ok := <-q.arrival
		if !ok {
			return nil, fmt.Errorf("blocking queue closed")
		}
	}
	top = q.popFirst()
	return top, nil
}

func (q *oneToOneBlockingQueue) popFirst() *Message {
	front := q.msgs.Front()
	q.msgs.Remove(front)
	return front.Value.(*Message)
}

func (q *oneToOneBlockingQueue) Close() {
	close(q.arrival)
}

type fixedSizeBlockingQueue struct {
	buffer chan *Message
}

func newFixedSizeBlockingQueue(size int) blockingQueue {
	return &fixedSizeBlockingQueue{
		buffer: make(chan *Message, size),
	}
}

func (q *fixedSizeBlockingQueue) Push(m *Message) {
	select {
	case q.buffer <- m:
	default:
	}
}

func (q *fixedSizeBlockingQueue) Error(e error) {

}

func (q *fixedSizeBlockingQueue) Pop() (*Message, error) {
	msg, ok := <-q.buffer
	if ok {
		return msg, nil
	} else {
		return nil, fmt.Errorf("blocking queue closed")
	}
}

func (q *fixedSizeBlockingQueue) Top() (*Message, error) {
	select {
	case msg := <-q.buffer:
		return msg, nil
	default:
		return nil, fmt.Errorf("empty queue")
	}
}

func (q *fixedSizeBlockingQueue) Close() {
	close(q.buffer)
}

type MsgPair struct {
	msg *Message
	err error
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

func (q *unboundedBlockingQueue) Push(m *Message) {
	q.push <- &MsgPair{m, nil}
}

func (q *unboundedBlockingQueue) Error(e error) {
	q.push <- &MsgPair{nil, e}
}

func (q *unboundedBlockingQueue) Pop() (*Message, error) {
	q.req <- struct{}{}
	p := <-q.res
	return p.msg, p.err
}

// not thread safe
func (q *unboundedBlockingQueue) Top() (*Message, error) {
	if q.queue.Len() == 0 {
		return nil, fmt.Errorf("empty queue")
	}
	front := q.queue.Front().Value.(*MsgPair)
	return front.msg, front.err
}

func (q *unboundedBlockingQueue) Close() {
	close(q.quit)
}
