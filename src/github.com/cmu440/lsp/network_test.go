package lsp

import (
	"testing"
)

// TODO figure out the reason of the werid behaviors of golang UDPConn

func TestWeirdClientBehavior(t *testing.T) {
	client, err := NewFixedSizeUDPClient(1024, "localhost:9923") // non-nil error even though server is not running
	defer client.Close()

	// if run the code below, client will block on read(), which is the desired behavior
	// data := make([]byte, 1024)
	// sz, err = client.Read(data)

	// However, when we run a write first (which is non-blocking and returns nil error, seems it has its own buffer)
	// and then run a Read(), this time it will return non-nil error complaining "connection refused"
	in := make([]byte, 1024)
	out := make([]byte, 256)

	_, err = client.Write(out)
	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = client.Read(in) // should return connection-refused error
	if err == nil {
		t.Errorf("expected non-nil error")
	}
}

func TestWeirdClientBehavior2(t *testing.T) {
	client, err := NewFixedSizeUDPClient(1024, "localhost:9923")
	defer client.Close()
	if err != nil {
		t.Errorf(err.Error())
	}

	server, err := NewFixedSizeUDPServer(1024, 9923)
	defer server.Close()
	if err != nil {
		t.Errorf(err.Error())
	}

	out := make([]byte, 256)
	sz, err := client.Write(out)
	if err != nil {
		t.Errorf(err.Error())
	}
	if sz != 256 {
		t.Errorf("wrong out size %v", sz)
	}

	sz, err = client.Write(out)
	if err != nil {
		t.Errorf(err.Error())
	}
	if sz != 256 {
		t.Errorf("wrong out size %v", sz)
	}

	in := make([]byte, 1024)
	sz, _, err = server.Read(in)
	if err != nil {
		t.Errorf(err.Error())
	}
	if sz != 256 {
		t.Errorf("wrong in size %v", sz)
	}

	sz, _, err = server.Read(in)
	if err != nil {
		t.Errorf(err.Error())
	}
	if sz != 256 {
		t.Errorf("wrong in size %v", sz)
	}

	// now it's blocking there if running client.Read() again
	/*
		data = make([]byte, 1024)
		sz, err = client.Read(data)
		log.Println(sz, err)
	*/
}
