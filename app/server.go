package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	connection, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer connection.Close()

	reader := bufio.NewReader(connection)
	var recv string
	var count int

	for {
		localRecv, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Connection closed by the server")
			} else {
				fmt.Printf("Failed to read string with error: %v\n", err)
			}
			os.Exit(1)
		}
		if len(recv) > 0 {
			recv += localRecv
		} else {
			recv = localRecv
		}
		if strings.HasSuffix(recv, "\r\n") {
			count += 1
			recv = ""
		}
		if count == 3 {
			connection.Write([]byte("+PONG\r\n"))
			count = 0
		}
	}
}
