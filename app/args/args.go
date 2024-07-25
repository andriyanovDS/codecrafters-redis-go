package args

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/replication"
)

type Args struct {
	Port        uint16
	ReplicaOf   replication.ReplicaAddress
	RdbDir      string
	RdbFileName string
	Raw         map[string]string
}

var parsers = map[string]flagParser{
	"port":       port,
	"replicaof":  replicaof,
	"dir":        rdbDir,
	"dbfilename": rdbFileName,
}

func ParseArgs() Args {
	osArgs := os.Args[1:]
	var args Args
	args.Raw = make(map[string]string)
	for {
		if len(osArgs) == 0 {
			break
		}
		flag, _ := strings.CutPrefix(osArgs[0], "--")
		parser := parsers[flag]
		if parser != nil {
			rest, value := parser(osArgs[1:], &args)
			osArgs = rest
			args.Raw[flag] = value
		}
	}
	if args.Port == 0 {
		args.Port = 6379
	}
	return args
}

type flagParser func(rest []string, args *Args) ([]string, string)

func port(rest []string, args *Args) ([]string, string) {
	if len(rest) == 0 {
		return rest, ""
	}
	num, err := strconv.ParseUint(rest[0], 10, 16)
	if err != nil {
		fmt.Printf("failed to parse port: %v", err)
	} else {
		args.Port = uint16(num)
	}
	return rest[1:], rest[0]
}

func replicaof(rest []string, args *Args) ([]string, string) {
	if len(rest) == 0 {
		return rest, ""
	}
	parts := strings.Split(rest[0], " ")
	if len(parts) == 2 {
		port, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			fmt.Printf("failed to parse replica port: %v", err)
		} else {
			args.ReplicaOf = replication.ReplicaAddress{
				Host: parts[0],
				Port: uint16(port),
			}
		}
	} else {
		fmt.Printf("Invalid replica address %v\n", rest[0])
	}
	return rest[1:], rest[0]
}

func rdbDir(rest []string, args *Args) ([]string, string) {
	if len(rest) == 0 {
		return rest, ""
	}
	args.RdbDir = rest[0]
	return rest[1:], rest[0]
}

func rdbFileName(rest []string, args *Args) ([]string, string) {
	if len(rest) == 0 {
		return rest, ""
	}
	args.RdbFileName = rest[0]
	return rest[1:], rest[0]
}
