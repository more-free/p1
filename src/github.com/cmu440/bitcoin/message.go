package bitcoin

/*
modified a little bit for the original structures :
for Request message (server -> worker), use Nonce field as task ID;
for Result message (worker -> server), use Lower field as task ID (the same task ID as request message);
for server -> client, client -> server, the ID field doesn't matter.

result ID is encoded to let the server identify which request it belongs to.
when result is sent from miner to server, the id indicates a unique subtask id
storing on the server side.

note normally it should modify the Message struct to add a separate ID field instead of encoding ID with trick.
*/

import "fmt"

type MsgType int

const (
	Join MsgType = iota
	Request
	Result
)

// Message represents a message that can be sent between components in the bitcoin
// mining distributed system. Messages must be marshalled into a byte slice before being
// sent over the network.
type Message struct {
	Type         MsgType
	Data         string
	Lower, Upper uint64
	Hash, Nonce  uint64
}

// NewRequest creates a request message. Clients send request messages to the
// server and the server sends request messages to miners.
func NewRequest(data string, lower, upper uint64) *Message {
	return &Message{
		Type:  Request,
		Data:  data,
		Lower: lower,
		Upper: upper,
	}
}

// New result creates a result message. Miners send result messages to the server
// and the server sends result messages to clients.
func NewResult(hash, nonce uint64) *Message {
	return &Message{
		Type:  Result,
		Hash:  hash,
		Nonce: nonce,
	}
}

// NewJoin creates a join message. Miners send join messages to the server.
func NewJoin() *Message {
	return &Message{Type: Join}
}

func (m *Message) String() string {
	var result string
	switch m.Type {
	case Request:
		result = fmt.Sprintf("[%s %s %d %d]", "Request", m.Data, m.Lower, m.Upper)
	case Result:
		result = fmt.Sprintf("[%s %d %d]", "Result", m.Hash, m.Nonce)
	case Join:
		result = fmt.Sprintf("[%s]", "Join")
	}
	return result
}

// helpers
func SubRequest(lower uint64, upper uint64, request *Message) *Message {
	return &Message{
		Type:  Request,
		Data:  request.Data,
		Lower: lower,
		Upper: upper,
	}
}

func (m *Message) SetID(id int) {
	switch m.Type {
	case Request:
		m.Nonce = uint64(id)
	case Result:
		m.Lower = uint64(id)
	}
}

func (m *Message) GetID() int {
	switch m.Type {
	case Request:
		return int(m.Nonce)
	case Result:
		return int(m.Lower)
	default:
		return -1
	}
}
