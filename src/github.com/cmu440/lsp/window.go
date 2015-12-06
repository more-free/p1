package lsp

import (
	"container/list"
	"fmt"
)

// each connection holds a window struct
type MsgWindow struct {
	messages *list.List // messages sent recently
	ack      map[int]bool
	capacity int
}

type AckWindow struct {
	acks     map[int]*Message // acks sent recently
	capacity int
}

type Window struct {
	*MsgWindow
	*AckWindow
}

type BySeqNum []*Message

func (s BySeqNum) Len() int {
	return len(s)
}

func (s BySeqNum) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s BySeqNum) Less(i, j int) bool {
	return s[i].SeqNum < s[j].SeqNum
}

func NewWindow(capacity int) *Window {
	msgWindow := &MsgWindow{
		list.New(),
		make(map[int]bool),
		capacity,
	}

	ackWindow := &AckWindow{
		make(map[int]*Message),
		capacity,
	}

	return &Window{
		msgWindow,
		ackWindow,
	}
}

func (mw *MsgWindow) MsgSize() int {
	return mw.messages.Len()
}

func (aw *AckWindow) AckSize() int {
	return len(aw.acks)
}

func (mw *MsgWindow) hasWaitingMessage() bool {
	has := false
	for _, received := range mw.ack {
		if !received {
			has = true
			break
		}
	}
	return has
}

func (mw *MsgWindow) IsValidToSend(msg *Message) bool {
	return mw.messages.Len() < mw.capacity ||
		mw.ack[mw.messages.Front().Value.(*Message).SeqNum]
}

func (mw *MsgWindow) hasSent(msg *Message) bool {
	_, exists := mw.ack[msg.SeqNum]
	return exists
}

func (mw *MsgWindow) SendMsg(msg *Message) error {
	if mw.hasSent(msg) { // ignore resending message
		return nil
	}

	if !mw.IsValidToSend(msg) {
		return fmt.Errorf("%v is a not valid seq num to send", msg.SeqNum)
	}

	if _, sent := mw.ack[msg.SeqNum]; sent {
		return nil // ignore duplicate message
	}

	mw.messages.PushBack(msg)
	mw.ack[msg.SeqNum] = false

	if mw.messages.Len() > mw.capacity {
		front := mw.messages.Front()
		delete(mw.ack, front.Value.(*Message).SeqNum)
		mw.messages.Remove(front)
	}
	return nil
}

func (mw *MsgWindow) GetMsgForResend() []*Message {
	msgs := make([]*Message, 0)
	for e := mw.messages.Front(); e != nil; e = e.Next() {
		seqNum := e.Value.(*Message).SeqNum
		if ack, exists := mw.ack[seqNum]; exists && !ack {
			msgs = append(msgs, e.Value.(*Message))
		}
	}

	return msgs
}

// may receive out-of-order or duplicated acks
func (mw *MsgWindow) ReceiveAck(ack *Message) error {
	if ack.SeqNum < mw.messages.Front().Value.(*Message).SeqNum {
		return fmt.Errorf("ack %v has been received", ack.SeqNum)
	}

	mw.ack[ack.SeqNum] = true
	return nil
}

// may receive duplicated messages
func (aw *AckWindow) HasMsgReceived(msg *Message) bool {
	if len(aw.acks) > 0 && msg.SeqNum < aw.getMinAck().SeqNum {
		return true
	} else {
		_, exist := aw.acks[msg.SeqNum]
		return exist
	}
}

// invoke after receiving a new message
// it may send out-of-order acks (due to receiving out-of-order messages)
// also, the message received are stored in order to deliver to the caller
// TODO or should name it ReceiveMsg
func (aw *AckWindow) SendAck(msg *Message) error {
	if aw.HasMsgReceived(msg) {
		return fmt.Errorf("ack %v has been sent", msg.SeqNum)
	}

	// save original msg instead of just ack, as recently received messages might
	// be resent later
	aw.acks[msg.SeqNum] = msg
	if len(aw.acks) > aw.capacity {
		// can safely delete min now
		delete(aw.acks, aw.getMinAck().SeqNum)
	}
	return nil
}

func (aw *AckWindow) GetAckForResend() []*Message {
	acks := make([]*Message, 0)
	for _, msg := range aw.acks {
		acks = append(acks, GetAck(msg))
	}
	return acks
}

// TODO : use heap to optimize the time complexity
func (aw *AckWindow) getMinAck() *Message {
	var minMsg *Message
	for _, ack := range aw.acks {
		if minMsg == nil || ack.SeqNum < minMsg.SeqNum {
			minMsg = ack
		}
	}
	return minMsg
}

// get recently received message by seqNum
func (aw *AckWindow) getMessage(seqNum int) *Message {
	msg, exists := aw.acks[seqNum]
	if !exists {
		return nil
	} else {
		return msg
	}
}
