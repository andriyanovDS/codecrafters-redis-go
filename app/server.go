package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/args"
	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Array = resp.Array

var redisCommands = []commands.Command{
	commands.EchoCommand,
	commands.PingCommand,
	commands.SetCommand,
	commands.GetCommand,
	commands.InfoCommand,
}

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
		go syncWithMaster(slaveRole)
	}
	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		fmt.Println("Connection accepted")
		go handleConnection(connection, &context)
	}
}

func handleConnection(connection net.Conn, context *commands.Context) {
	defer connection.Close()

	reader := bufio.NewReader(connection)
	for {
		resp, err := resp.Parse(reader)
		if err != nil {
			fmt.Printf("RESP parsing failed: %v\n", err)
			return
		}
		request := resp.(Array)
		for _, command := range redisCommands {
			response := command(request, context)
			if response != nil {
				connection.Write(response.Bytes())
			}
		}
	}
}

func syncWithMaster(slave replication.SlaveRole) {
	conn, err := replication.ConnectToMaster(slave)
	if err != nil {
		fmt.Printf("failed to establish connection with master: %v", err)
		return
	}
	conn.Handshake()
}
