package lsp

import (
	"runtime"
	"testing"
)

func TestUnbounedBlockingQueue(t *testing.T) {
	runtime.GOMAXPROCS(2)

	q := newUnboundedBlockingQueue()
	defer q.Close()
	size := 100
	res := make(chan *Message, size)

	push := func(id int) {
		q.Push(GetMessage(id, 0, EmptyPayload()))
	}

	pop := func() {
		msg, _ := q.Pop()
		res <- msg
	}

	for id := 0; id < size; id++ {
		go push(id)
	}

	for i := 0; i < size; i++ {
		go pop()
	}

	get := make(map[int]*Message)
	for i := 0; i < size; i++ {
		msg := <-res
		t.Log(msg)
		get[msg.ConnID] = msg
	}

	if len(get) != size {
		t.Errorf("expected %v but %v", size, len(get))
	}
}
