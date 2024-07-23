package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/args"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Array = resp.Array
type BulkString = resp.BulkString

func main() {
	args := args.ParseArgs()
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", args.Port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", args.Port)
		os.Exit(1)
	}
	fmt.Printf("Listening on port %v\n", args.Port)

	context := commands.NewContext(args)
	slaveRole, ok := context.ReplicationRole.(replication.SlaveRole)
	if ok {
		go syncWithMaster(&slaveRole, args.Port, &context)
	}
	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		fmt.Println("Connection accepted")
		go func() {
			defer connection.Close()
			reader := bufio.NewReader(connection)
			listenCommands(reader, connection, &context)
		}()
	}
}

func listenCommands(reader *bufio.Reader, writer io.Writer, context *commands.Context) {
	for {
		resp, err := resp.Parse(reader)
		if err != nil {
			fmt.Printf("RESP parsing failed: %s\n", err)
			return
		}
		request, ok := resp.(Array)
		if !ok {
			fmt.Printf("ignored command: %v\n", resp)
			continue
		}
		if len(request.Content) == 0 {
			continue
		}
		command := string(request.Content[0].(BulkString))
		handler, ok := commands.Commands[strings.ToLower(command)]
		if !ok {
			fmt.Printf("unknown command received: %v\n", command)
			continue
		}
		err = handler(request.Content[1:], resp, writer, context)
		if err != nil {
			fmt.Printf("%s command handling failure: %v\n", command, err)
			continue
		}
	}
}

func syncWithMaster(slave *replication.SlaveRole, listeningPort uint16, context *commands.Context) {
	conn, err := replication.ConnectToMaster(slave)
	if err != nil {
		fmt.Printf("failed to establish connection with master: %v\n", err)
		return
	}
	fmt.Printf("connection with master established. Port: %d\n", listeningPort)
	reader, err := conn.Handshake(listeningPort)
	if err != nil {
		fmt.Printf("handshake failed: %v\n", err)
	}
	fmt.Printf("handshake with master completed. Port: %d\n", listeningPort)
	listenCommands(reader, conn, context)
}
