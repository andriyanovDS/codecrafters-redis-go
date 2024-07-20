package args

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Args struct {
	Port      uint16
	ReplicaOf ReplicaAddress
}

type ReplicaAddress struct {
	Host string
	Port uint16
}

var parsers = map[string]flagParser{
	"port":      port,
	"replicaof": replicaof,
}

func ParseArgs() Args {
	osArgs := os.Args[1:]
	var args Args
	for {
		if len(osArgs) == 0 {
			break
		}
		flag, _ := strings.CutPrefix(osArgs[0], "--")
		parser := parsers[flag]
		if parser != nil {
			osArgs = parser(osArgs[1:], &args)
		}
	}
	if args.Port == 0 {
		args.Port = 6379
	}
	return args
}

type flagParser func(rest []string, args *Args) []string

func port(rest []string, args *Args) []string {
	if len(rest) == 0 {
		return rest
	}
	num, err := strconv.ParseUint(rest[0], 10, 16)
	if err != nil {
		fmt.Printf("failed to parse port: %v", err)
	} else {
		args.Port = uint16(num)
	}
	return rest[1:]
}

func replicaof(rest []string, args *Args) []string {
	if len(rest) == 0 {
		return rest
	}
	parts := strings.Split(rest[0], " ")
	if len(parts) == 2 {
		port, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			fmt.Printf("failed to parse replica port: %v", err)
		} else {
			args.ReplicaOf = ReplicaAddress{
				Host: parts[0],
				Port: uint16(port),
			}
		}
	} else {
		fmt.Printf("Invalid replica address %v\n", rest[0])
	}
	return rest[1:]
}
