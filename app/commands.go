package main

import (
	"strings"
	"sync"
)

type Command func(Array, *Context) RespDataType

type Context struct {
	storage map[string]string
	mutex   sync.Mutex
}

func NewContext() Context {
	return Context{
		storage: make(map[string]string),
		mutex:   sync.Mutex{},
	}
}

func PingCommand(resp Array, _ *Context) RespDataType {
	len := len(resp.Content)
	if len == 0 || len > 2 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(command.string) != "ping" {
		return nil
	}
	if len == 2 {
		return resp.Content[1]
	} else {
		return BulkString{"PONG"}
	}
}

func EchoCommand(resp Array, _ *Context) RespDataType {
	if len(resp.Content) != 2 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(command.string) != "echo" {
		return nil
	}
	return resp.Content[1]
}

func SetCommand(resp Array, context *Context) RespDataType {
	if len(resp.Content) == 0 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(command.string) != "set" {
		return nil
	}
	key := toString(resp.Content[1])
	value := toString(resp.Content[2])

	context.mutex.Lock()
	context.storage[key] = value
	context.mutex.Unlock()
	return SimpleString{"OK"}
}

func GetCommand(resp Array, context *Context) RespDataType {
	if len(resp.Content) != 2 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(command.string) != "get" {
		return nil
	}
	key := toString(resp.Content[1])

	context.mutex.Lock()
	value, ok := context.storage[key]
	context.mutex.Unlock()

	if ok {
		return BulkString{value}
	} else {
		return BulkString{"-1"}
	}
}

func toString(r RespDataType) string {
	switch t := r.(type) {
	case BulkString:
		return t.string
	case SimpleString:
		return t.string
	default:
		return ""
	}
}
