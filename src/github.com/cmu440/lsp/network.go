package lsp

import (
	"fmt"
	net "github.com/cmu440/lspnet"
)

type UDPClient interface {
	Read(data []byte) (int, error)
	Write(data []byte) (int, error)
	Close() error
	MaxMsgSize() int
}

type UDPServer interface {
	Read(data []byte) (int, *net.UDPAddr, error)
	Write(data []byte, clientAddr *net.UDPAddr) (int, error)
	Close() error
	MaxMsgSize() int
}

// assume message size is fixed for this project
type FixedSizeUDPClient struct {
	maxMsgSize int
	serverAddr *net.UDPAddr
	conn       *net.UDPConn
	closed     bool
}

type FixedSizeUDPServer struct {
	maxMsgSize int
	addr       *net.UDPAddr // self addr
	conn       *net.UDPConn // receive multiple clients' requests on the same conn
	closed     bool         // if server is shut down
}

func NewFixedSizeUDPClient(maxMsgSize int, serverHostPort string) (UDPClient, error) {
	serverAddr, err := net.ResolveUDPAddr("udp4", serverHostPort)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, serverAddr) // non-blocking. it won't wait for server to build connection as TCP does
	if err != nil {
		return nil, err
	}

	return &FixedSizeUDPClient{
		maxMsgSize: maxMsgSize,
		serverAddr: serverAddr,
		conn:       conn,
		closed:     false,
	}, nil
}

func (c *FixedSizeUDPClient) Read(data []byte) (int, error) {
	if len(data) < c.maxMsgSize {
		return 0, fmt.Errorf("buffer size is less than max message size %v", c.maxMsgSize)
	}

	if c.closed {
		return 0, fmt.Errorf("client is closed")
	}

	return c.conn.Read(data)
}

func (c *FixedSizeUDPClient) Write(data []byte) (int, error) {
	if c.closed {
		return 0, fmt.Errorf("client is closed")
	}

	return c.conn.Write(data)
}

func (c *FixedSizeUDPClient) Close() error {
	if c.closed {
		return fmt.Errorf("client is closed")
	}

	c.conn.Close()
	c.closed = true
	return nil
}

func (c *FixedSizeUDPClient) MaxMsgSize() int {
	return c.maxMsgSize
}

func NewFixedSizeUDPServer(maxMsgSize int, port int) (UDPServer, error) {
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr) // non-blocking. no need to wait for any client to establish a connection like TCP does
	if err != nil {
		return nil, err
	}

	return &FixedSizeUDPServer{
		maxMsgSize: maxMsgSize,
		addr:       addr,
		conn:       conn,
		closed:     false,
	}, nil
}

func (s *FixedSizeUDPServer) Read(data []byte) (int, *net.UDPAddr, error) {
	if len(data) < s.maxMsgSize {
		return 0, nil, fmt.Errorf("buffer size is less than max message size %v", s.maxMsgSize)
	}

	if s.closed {
		return 0, nil, fmt.Errorf("server is closed")
	}

	return s.conn.ReadFromUDP(data)
}

func (s *FixedSizeUDPServer) Write(data []byte, clientAddr *net.UDPAddr) (int, error) {
	if s.closed {
		return 0, fmt.Errorf("server is closed")
	}

	return s.conn.WriteToUDP(data, clientAddr)
}

func (s *FixedSizeUDPServer) Close() error {
	if s.closed {
		return fmt.Errorf("server is closed")
	}

	s.conn.Close()
	s.closed = true
	return nil
}

func (s *FixedSizeUDPServer) MaxMsgSize() int {
	return s.maxMsgSize
}
