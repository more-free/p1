package main

import (
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

type Client struct {
	client lsp.Client
	inMsg  chan *bitcoin.Message // incoming messages
	done   chan bool
}

func NewClient(hostport string, params *lsp.Params, done chan bool) (*Client, error) {
	client, err := lsp.NewClient(hostport, params)
	if err != nil {
		return nil, err
	}

	c := &Client{
		client: client,
		inMsg:  make(chan *bitcoin.Message),
		done:   done,
	}

	return c, nil
}

func (c *Client) Start() {
	go func() {
		for {
			data, err := c.client.Read()
			if err != nil {
				log.Printf("quit client due to %v", err)
				break
			}

			msg, err := bitcoin.FromBytes(data)
			if err != nil {
				log.Printf("invalid message from server %v", err)
				continue
			}

			c.handleMessage(msg)
		}

		printDisconnected()
		c.done <- true
	}()
}

func (c *Client) Stop() {
	c.client.Close()
}

func (c *Client) handleMessage(m *bitcoin.Message) {
	switch m.Type {
	case bitcoin.Result:
		c.handleResult(m)
	default:
		log.Println("invalid message type", m.Type)
	}
}

func (c *Client) handleResult(m *bitcoin.Message) {
	printResult(strconv.FormatUint(m.Hash, 10), strconv.FormatUint(m.Nonce, 10))
}

func (c *Client) SendRequest(data string, upper uint64) error {
	req := bitcoin.NewRequest(data, 0, upper)
	bytes, _ := req.ToBytes()
	return c.client.Write(bytes)
}

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}

	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, _ := strconv.Atoi(os.Args[3])
	upper := uint64(maxNonce)
	done := make(chan bool)

	client, err := NewClient(hostport, lsp.NewParams(), done)
	if err != nil {
		fmt.Println("failed to start client", err)
		return
	}

	client.Start()
	client.SendRequest(message, upper)
	<-done
	client.Stop()
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
