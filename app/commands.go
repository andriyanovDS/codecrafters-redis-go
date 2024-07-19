package main

import (
	"strings"
)

type Command func(Array) RespDataType

func PingCommand(resp Array) RespDataType {
	len := len(resp.Content)
	if len == 0 || len > 2 {
		return nil
	}
	command := resp.Content[0].(String)
	if strings.ToLower(command.Content) != "ping" {
		return nil
	}
	if len == 2 {
		return resp.Content[1].(String)
	} else {
		return String{Content: "PONG"}
	}
}

func EchoCommand(resp Array) RespDataType {
	if len(resp.Content) != 2 {
		return nil
	}
	command := resp.Content[0].(String)
	if strings.ToLower(command.Content) != "echo" {
		return nil
	}
	return resp.Content[1].(String)
}
