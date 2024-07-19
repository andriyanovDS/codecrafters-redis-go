package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

var commands = []Command{
	EchoCommand,
	PingCommand,
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		fmt.Println("Connection accepted")
		go handleConnection(connection)
	}
}

func handleConnection(connection net.Conn) {
	defer connection.Close()

	reader := bufio.NewReader(connection)
	for {
		resp, err := Parse(reader)
		if err != nil {
			fmt.Printf("RESP parsing failed: %v\n", err)
			return
		}
		request := resp.(Array)
		for _, command := range commands {
			response := command(request)
			if response != nil {
				connection.Write(response.Bytes())
			}
		}
	}
}
