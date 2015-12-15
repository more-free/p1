package main

import (
	"fmt"
	"net"
	"time"
)

const (
	SERVER   = ":5051"
	MAX_SIZE = 1024
)

func startClient() {
	serverAddr, _ := net.ResolveUDPAddr("udp4", SERVER)
	conn, err := net.DialUDP("udp", nil, serverAddr)

	fmt.Println("client is running", err)

	data := make([]byte, MAX_SIZE)
	if err == nil {
		_, err = conn.Write([]byte("hello, world"))
		fmt.Println("client", err)

		conn.Close()
		fmt.Println("client conn closed")
		time.Sleep(time.Second * 3)

		sz, err := conn.Read(data)
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("client read ", sz, string(data))
		}
	} else {
		fmt.Println(err.Error())
	}
}

func startServer() {
	selfAddr, err := net.ResolveUDPAddr("udp4", SERVER)
	if err == nil {
		fmt.Println("server is running", err)

		conn, _ := net.ListenUDP("udp", selfAddr)

		fmt.Println("start listening")

		data := make([]byte, MAX_SIZE)
		sz, clientAddr, _ := conn.ReadFromUDP(data)
		fmt.Println(clientAddr.String())

		msg := string(data[:sz])
		fmt.Println("server receives msg", msg)
		time.Sleep(time.Second * 2)
		fmt.Println("server start to write")

		// echo
		_, err = conn.WriteToUDP([]byte(msg), clientAddr)
		fmt.Println(err)

		// close
		//conn.Close()
		//fmt.Println("server udp conn closed")

	} else {
		fmt.Println(err.Error())
	}
}

func main() {
	go startServer()
	time.Sleep(time.Second * 1)

	startClient()

}
