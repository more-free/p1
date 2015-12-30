package main

import (
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
)

type Miner struct {
	client lsp.Client
}

func NewMiner(hostport string, params *lsp.Params) (*Miner, error) {
	client, err := lsp.NewClient(hostport, params)
	if err != nil {
		return nil, fmt.Errorf("cannot create miner :%v", err)
	}

	return &Miner{
		client: client,
	}, nil
}

func (m *Miner) Start() {
	for {
		bytes, err := m.client.Read()
		if err != nil {
			log.Printf("quit miner due to :%v", err)
			break
		}

		msg, err := bitcoin.FromBytes(bytes)
		if err != nil {
			continue // ignore invalid request
		}

		m.handleMessage(msg)
	}

	log.Printf("quit miner")
}

func (m *Miner) Stop() {
	m.client.Close()
}

func (m *Miner) handleMessage(msg *bitcoin.Message) {
	switch msg.Type {
	case bitcoin.Request:
		log.Printf("processing request %v", msg)
		m.handleRequest(msg)
	default:
		log.Printf("unsupported message type")
	}
}

func (m *Miner) handleRequest(req *bitcoin.Message) {
	go func() {
		nonce := req.Lower
		minHash := bitcoin.Hash(req.Data, nonce)

		for n := req.Lower + 1; n <= req.Upper; n++ {
			hash := bitcoin.Hash(req.Data, n)
			if hash < minHash {
				minHash = hash
				nonce = n
			}
		}

		res := bitcoin.NewResult(minHash, nonce)
		res.SetID(req.GetID())
		m.write(res)
	}()
}

func (m *Miner) write(msg *bitcoin.Message) {
	bytes, _ := msg.ToBytes()
	err := m.client.Write(bytes)
	if err != nil {
		log.Printf("client error %v", err)
		m.Stop()
	}
}

func (m *Miner) Join() {
	m.write(bitcoin.NewJoin())
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	miner, err := NewMiner(os.Args[1], lsp.NewParams())
	if err != nil {
		log.Printf("failed to start miner %v", err)
		return
	}

	miner.Join()
	miner.Start()
}
