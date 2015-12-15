/*
implementation of lsp protocol, shared by both sides (a peer-to-peer protocol rather than C/S)
- sliding window bound read/write
- epoch driven resend data/ack
- unbounded buffer size for pending incoming/outgoing data/ack
- any part that is not shared by both sides are excluded, ex., building connection (client initiated)

TODO optimize with tree or similar
TODO methods are not thread-safe
TODO too many blocking operations
*/
package lsp

import (
	"container/list"
	"fmt"
	"math"
	"time"
)

type MsgReadWriter interface {
	Read() (*Message, error)
	Write(msg *Message) error
	ConnID() int
	Close() error
}

// data already received but not yet delivered due to out-of-order arrival
type incomingData struct {
	data map[int]*Message
}

func newIncomingData() *incomingData {
	return &incomingData{
		data: make(map[int]*Message),
	}
}

func (c *incomingData) Contains(id int) bool {
	_, exists := c.data[id]
	return exists
}

func (c *incomingData) Poll(id int) (*Message, error) {
	if c.Contains(id) {
		msg := c.data[id]
		delete(c.data, id)
		return msg, nil
	} else {
		return nil, fmt.Errorf("id %v does not exist", id)
	}
}

// poll min data which behaves like a FIFO queue
func (c *incomingData) PollMin() (*Message, error) {
	if c.Len() == 0 {
		return nil, fmt.Errorf("polling from empty queu")
	}

	minId := c.getMinId()
	delete(c.data, minId)
	return c.data[minId], nil
}

func (c *incomingData) getMinId() int {
	minId := math.MaxInt32
	for id, _ := range c.data {
		if id < minId {
			minId = id
		}
	}
	return minId
}

func (c *incomingData) Push(msg *Message) {
	c.data[msg.SeqNum] = msg
}

func (c *incomingData) Len() int {
	return len(c.data)
}

// FIFO queue for saved but not yet sent data due to sliding window limitation
type outgoingData struct {
	data *list.List
}

func newOutgoingData() *outgoingData {
	return &outgoingData{
		data: list.New(),
	}
}

func (c *outgoingData) Front() (*Message, error) {
	if c.Len() == 0 {
		return nil, fmt.Errorf("queue is empty")
	}

	return c.data.Front().Value.(*Message), nil
}

func (c *outgoingData) Poll() (*Message, error) {
	if c.Len() == 0 {
		return nil, fmt.Errorf("polling from empty queue")
	}

	front := c.data.Front()
	c.data.Remove(front)
	return front.Value.(*Message), nil
}

func (c *outgoingData) Push(data *Message) {
	c.data.PushBack(data)
}

func (c *outgoingData) PushFront(data *Message) {
	c.data.PushFront(data)
}

func (c *outgoingData) Len() int {
	return c.data.Len()
}

// recently sent but unacknowledged data. max size == max window size
// enlarge when sending data; shrink when receiving ack
// some data structure like LinkedHashMap (java) is best for the task
type recentDataWindow struct {
	maxSize int
	data    map[int]*Message
}

func newRecentDataWindow(maxSize int) *recentDataWindow {
	return &recentDataWindow{
		maxSize: maxSize,
		data:    make(map[int]*Message),
	}
}

func (c *recentDataWindow) IsValid(data *Message) bool {
	return c.Len() == 0 || data.SeqNum-c.getMinId() < c.maxSize
}

func (c *recentDataWindow) Push(data *Message) error {
	if !c.IsValid(data) {
		return fmt.Errorf("data window is full")
	}
	c.data[data.SeqNum] = data
	return nil
}

func (c *recentDataWindow) Remove(id int) {
	if _, exists := c.data[id]; exists {
		delete(c.data, id)
	}
}

func (c *recentDataWindow) GetAll() []*Message {
	msgs := make([]*Message, 0)
	for _, msg := range c.data {
		msgs = append(msgs, msg)
	}
	return msgs
}

func (c *recentDataWindow) Len() int {
	return len(c.data)
}

func (c *recentDataWindow) getMinId() int {
	minId := math.MaxInt32
	for id, _ := range c.data {
		if id < minId {
			minId = id
		}
	}
	return minId
}

// ack for recently received data. max size == max window size
// enlarge/auto-shrink when sending a new ack
type recentAckWindow struct {
	maxSize int
	ack     map[int]*Message
}

func newRecentAckWindow(maxSize int) *recentAckWindow {
	return &recentAckWindow{
		maxSize: maxSize,
		ack:     make(map[int]*Message),
	}
}

func (c *recentAckWindow) Push(ack *Message) {
	_, exists := c.ack[ack.SeqNum]
	if !exists {
		c.ack[ack.SeqNum] = ack
		c.shrink(ack)
	}
}

func (c *recentAckWindow) shrink(ack *Message) {
	for id, _ := range c.ack {
		if ack.SeqNum-c.maxSize >= id {
			delete(c.ack, id)
		}
	}
}

func (c *recentAckWindow) GetAll() []*Message {
	acks := make([]*Message, 0)
	for _, ack := range c.ack {
		acks = append(acks, ack)
	}
	return acks
}

// Note its methods are NOT thread-safe
type LSPRunner struct {
	rw             MsgReadWriter
	params         *Params
	nextRead       int
	nextWrite      int
	incomingData   *incomingData
	outgoingData   *outgoingData
	sentDataWindow *recentDataWindow
	sentAckWindow  *recentAckWindow
	inBuffer       blockingQueue
	closed         bool

	// event signals
	epochAlive chan struct{}
	msgArrival chan *Message
	close      chan struct{} // broadcasting close signal
	done       chan bool     // ready to close, where false means quit with error
}

func NewLSPRunner(rw MsgReadWriter, params *Params) *LSPRunner {
	return &LSPRunner{
		rw:             rw,
		params:         params,
		nextRead:       1,
		nextWrite:      1,
		closed:         false,
		inBuffer:       newUnboundedBlockingQueue(),
		incomingData:   newIncomingData(),
		outgoingData:   newOutgoingData(),
		sentDataWindow: newRecentDataWindow(params.WindowSize),
		sentAckWindow:  newRecentAckWindow(params.WindowSize),
		epochAlive:     make(chan struct{}),
		msgArrival:     make(chan *Message),
		close:          make(chan struct{}),
		done:           make(chan bool),
	}
}

/*
Send data. Non-blocking, unbounded.
If sliding window is full (no ack received for the first element in the window), then save outgoing data to
pending queue, and return nil;
If LSPRunner is closed, return error;
Otherwise, write() returns successfully, the outgoing data is saved for future resend.
*/
func (r *LSPRunner) Write(payload []byte) error {
	if r.closed {
		return fmt.Errorf("writing data to closed LSPRunner")
	}

	data := GetMessage(r.rw.ConnID(), r.nextWrite, payload)
	err := r.sentDataWindow.Push(data)
	if err != nil { // sliding window is full
		r.outgoingData.Push(data)
		r.sentDataWindow.Remove(data.SeqNum)
		r.nextWrite++
		return nil
	}

	err = r.rw.Write(data)
	if err != nil {
		r.sentDataWindow.Remove(data.SeqNum) // roll back
		return err
	} else {
		r.nextWrite++
		return nil
	}
}

/*
blocking. return the next expected data.
*/
func (r *LSPRunner) Read() ([]byte, error) {
	msg, err := r.inBuffer.Pop()
	if err != nil {
		return EmptyPayload(), err
	} else {
		return msg.Payload, nil
	}
}

/*
blocking. return the next expected data.
*/
func (r *LSPRunner) readNext() ([]byte, error) {
	empty := make([]byte, 0)

	if r.closed {
		return empty, fmt.Errorf("reading from closed LSPRunner")
	}

	// if already in incoming data, return it
	if r.incomingData.Contains(r.nextRead) {
		msg, err := r.incomingData.Poll(r.nextRead)
		r.nextRead++
		return msg.Payload, err
	}

	for !r.closed {
		msg, err := r.rw.Read()
		if err != nil { // rw is closed either due to epoch timeout, or due to an explicit close()
			return empty, err
		}

		r.keepEpochAlive() // we received data, so keep epoch alive

		if IsAck(msg) {
			r.receiveAck(msg)
			r.notifyAckArrival(msg)
		} else if IsData(msg) {
			// the received data is what we want (i.e., it has 'nextRead' seqNum)
			if payload, err := r.receiveData(msg); err == nil {
				return payload, nil
			}
		} else {
			return empty, fmt.Errorf("only ack or data message is allowed") // should never happen
		}
	}

	return empty, fmt.Errorf("reading from closed LSPRunner")
}

func (r *LSPRunner) keepEpochAlive() {
	r.epochAlive <- struct{}{}
}

func (r *LSPRunner) notifyAckArrival(msg *Message) {
	r.msgArrival <- msg
}

/*
update data window, trigger resending pending data queue immediately
*/
func (r *LSPRunner) receiveAck(ack *Message) {
	// shrink the data window
	r.sentDataWindow.Remove(ack.SeqNum)

	r.resendOutgoingData() // go ?
}

func (r *LSPRunner) resendOutgoingData() error {
	for r.outgoingData.Len() > 0 {
		data, _ := r.outgoingData.Front()
		if r.sentDataWindow.IsValid(data) {
			err := r.rw.Write(data)
			// stop resending whenever an error occurs, leaving it to the next epoch
			// this is because we assume out-of-order sending is disallowed on the sender's side
			if err != nil {
				return fmt.Errorf("resend failed %v", err)
			}

			r.sentDataWindow.Push(data) // will not fail as we already validate it
			r.outgoingData.Poll()
		} else {
			return fmt.Errorf("no more valid data to resend")
		}
	}

	return nil
}

/*
Send Ack, update ack window (save ack for future resend),
if data is the next expected one, return it to caller; otherwise save data to received queue
*/
func (r *LSPRunner) receiveData(data *Message) ([]byte, error) {
	empty := make([]byte, 0)

	// send ack whenever a data message arrives
	ack := GetAck(data)
	if err := r.rw.Write(ack); err != nil {
		return empty, fmt.Errorf("writing ack error %v", err)
	}

	// save ack for future resend
	r.sentAckWindow.Push(ack)

	// return payload if data is expected, otherwise return an error
	if data.SeqNum == r.nextRead {
		r.nextRead++
		return data.Payload, nil
	} else {
		r.incomingData.Push(data)
		return empty, fmt.Errorf("receiving out-of-order data")
	}
}

/*
Resend recent unacknowledged data; resend recent ack; resend pending data queue.
this is a read-only method, so race condition won't be an issue
*/
func (r *LSPRunner) resend() {
	for _, data := range r.sentDataWindow.GetAll() {
		r.rw.Write(data)
	}

	for _, ack := range r.sentAckWindow.GetAll() {
		r.rw.Write(ack)
	}

	r.resendOutgoingData()
}

func (r *LSPRunner) Start() {
	go r.startReader()
	go r.startEpoch()
}

func (r *LSPRunner) startReader() {
	for {
		payload, err := r.readNext()
		if err == nil {
			r.inBuffer.Push(Msg(payload))
		} else { // closeReader() was called
			break
		}
	}
}

/*
Start epoch. blocking.
*/
func (r *LSPRunner) startEpoch() {
	inactiveEpoch := 0
	received := false
	closing := false
	isReadyToClose := func() bool {
		return r.outgoingData.Len() == 0 &&
			r.sentDataWindow.Len() == 0
	}

	for {
		select {
		case <-r.epochAlive:
			received = true

		case <-r.close:
			closing = true
			if isReadyToClose() {
				r.done <- true
				return
			}

		case <-r.msgArrival:
			// close gracefully
			if closing && isReadyToClose() {
				r.done <- true
				return
			}

		case <-time.After(time.Duration(r.params.EpochMillis) * time.Millisecond):
			if received {
				inactiveEpoch = 0
			} else {
				inactiveEpoch++
			}
			received = false

			if inactiveEpoch < r.params.EpochLimit {
				go r.resend()
			} else {
				break // epoch timeout
			}
		}
	}

	// Close self due to epoch timeout
	fmt.Println("epoch timeout, should not happen")
	r.stopNow()
}

func (r *LSPRunner) stopReader() {
	r.rw.Close()
}

func (r *LSPRunner) stopEpoch() {
	r.close <- struct{}{}
	<-r.done
}

/*
close gracefully. only stop when there is no outgoing data (previously failed to send) or unacknowledged data.
*/
func (r *LSPRunner) Stop() error {
	if r.closed {
		return fmt.Errorf("LSPRunner has been closed")
	}

	r.stopReader()
	r.stopEpoch() // TODO race condition with stopNow() called in startEpoch()

	r.inBuffer.Close()
	r.closed = true
	return nil
}

// do not wait for epoch to stop gracefully
func (r *LSPRunner) stopNow() {
	r.closed = true
	r.stopReader()
}
