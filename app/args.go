package main

import (
	"fmt"
	"os"
	"strconv"
)

type Args struct {
	Port uint16
}

func ParseArgs() Args {
	osArgs := os.Args[1:]
	var args Args
	for i := 0; i < len(osArgs); i++ {
		arg := osArgs[i]
		if arg == "--port" && i+1 < len(osArgs) {
			i += 1
			num, err := strconv.ParseUint(osArgs[i], 10, 16)
			if err != nil {
				fmt.Printf("failed to parse port: %v", err)
				continue
			}
			args.Port = uint16(num)
		}
	}
	if args.Port == 0 {
		args.Port = 6379
	}
	return args
}
