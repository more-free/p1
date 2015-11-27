package lsp

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func newMockMsg(seqNum int) *Message {
	var payload []byte
	return &Message{
		MsgData,
		0,
		seqNum,
		payload,
	}
}

func TestOrderedMsgAck(t *testing.T) {
	assert := assert.New(t)

	client := NewWindow(2)
	server := NewWindow(2)

	one := newMockMsg(1)
	two := newMockMsg(2)
	three := newMockMsg(3)

	client.SendMsg(one)
	client.SendMsg(two)

	// should return error due to limit of window size
	assert.Equal(false, client.IsValidToSend(three))

	server.SendAck(one)
	server.SendAck(two)
	client.ReceiveAck(one)

	// should unblock now
	assert.Equal(true, client.IsValidToSend(three))

	client.ReceiveAck(two)
	assert.Equal(true, server.HasMsgReceived(one))
	assert.Equal(true, server.HasMsgReceived(two))

	client.SendMsg(three)
	assert.Equal(2, client.MsgSize())
	assert.Equal(2, server.AckSize())

	server.SendAck(three)
	client.ReceiveAck(three)
	assert.Equal(2, server.AckSize())
}

func TestOutOfOrderMsgAck(t *testing.T) {
	assert := assert.New(t)

	client := NewWindow(2)
	server := NewWindow(2)

	one := newMockMsg(1)
	two := newMockMsg(2)
	three := newMockMsg(3)

	client.SendMsg(one)
	client.SendMsg(two)

	server.SendAck(two) // assume server receives two before one
	server.SendAck(one)

	assert.Equal(false, client.IsValidToSend(three))

	client.ReceiveAck(two)
	client.ReceiveAck(one)

	assert.Equal(true, client.IsValidToSend(three))

	client.SendMsg(three)
	assert.Equal(2, client.MsgSize())
}

func TestDuplicateMsg(t *testing.T) {
	assert := assert.New(t)

	client := NewWindow(2)
	server := NewWindow(2)

	one := newMockMsg(1)
	two := newMockMsg(2)

	client.SendMsg(one)
	client.SendMsg(one)
	client.SendMsg(one)
	client.SendMsg(two)
	assert.Equal(2, client.MsgSize())

	server.SendAck(one)
	server.SendAck(one)
	server.SendAck(one)
	client.ReceiveAck(one)
	client.ReceiveAck(one)
	client.ReceiveAck(one)

	assert.Equal(1, server.AckSize())
	assert.Equal(1, len(client.GetMsgForResend()))
}

func TestDuplicateAck(t *testing.T) {
	assert := assert.New(t)

	client := NewWindow(2)
	server := NewWindow(2)

	one := newMockMsg(1)
	two := newMockMsg(2)
	three := newMockMsg(3)

	client.SendMsg(one)
	client.SendMsg(two)

	server.SendAck(one)
	server.SendAck(one)
	assert.Equal(false, client.IsValidToSend(three))
	assert.Equal(1, len(server.GetAckForResend()))

	client.ReceiveAck(one)
	client.ReceiveAck(one)

	assert.Equal(2, client.MsgSize())
	assert.Equal(0, client.AckSize())
	assert.Equal(1, server.AckSize())
	assert.Equal(true, client.IsValidToSend(three))

	server.SendAck(two)
	client.ReceiveAck(two)
	client.SendMsg(three)
	server.SendAck(three)
	client.ReceiveAck(three)

	assert.Equal(2, len(server.GetAckForResend()))
	acks := server.GetAckForResend()
	sort.Sort(BySeqNum(acks))
	assert.Equal(2, acks[0].SeqNum)
	assert.Equal(3, acks[1].SeqNum)
}
