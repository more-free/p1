package lsp

import (
	"runtime"
	"testing"
)

func TestUnbounedBlockingQueue(t *testing.T) {
	runtime.GOMAXPROCS(2)

	q := newUnboundedBlockingQueue()
	size := 100
	res := make(chan *Message, size)

	push := func(id int) {
		q.Push(GetMessage(id, 0, EmptyPayload()))
	}

	pop := func() {
		msg, err := q.Pop()
		if err == nil {
			res <- msg
		} else {
			res <- nil
		}
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

	// pop should return non-nil error if the queue is closed
	for i := 0; i < size; i++ {
		go pop()
	}

	q.Close()
	for i := 0; i < size; i++ {
		msg := <-res
		if msg != nil {
			t.Errorf("expected nil message indicating non-nil error received")
		}
	}
}
