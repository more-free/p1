package lsp

import (
	"encoding/json"
	"fmt"
)

const (
	MAX_MSG_SIZE     = 1024
	MAX_WRITE_BUFFER = 100
)

func (m *Message) ToBytes() ([]byte, error) {
	return json.Marshal(m)
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
		Type:   MsgConnect,
		ConnID: connID,
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

func CheckError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}
