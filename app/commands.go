package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Command func(Array, *Context) RespDataType

type Context struct {
	storage map[string]entity
	mutex   sync.Mutex
}

type entity struct {
	value    string
	expireAt time.Time
}

func NewContext() Context {
	return Context{
		storage: make(map[string]entity),
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
	entity := entity{
		value:    toString(resp.Content[2]),
		expireAt: time.Time{},
	}

	var argIndex = 3
	for {
		if argIndex >= len(resp.Content) {
			break
		}
		func() {
			defer func() { argIndex += 2 }()
			arg := toString(resp.Content[argIndex])
			if strings.ToLower(arg) == "px" {
				ms, err := strconv.Atoi(toString(resp.Content[argIndex+1]))
				if err != nil {
					fmt.Printf("expiry time must be a positive integer")
					return
				}
				entity.expireAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
				fmt.Printf("Set expiry date: %v\n", entity.expireAt)
			}
		}()
	}

	context.mutex.Lock()
	context.storage[key] = entity
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
	entity, ok := context.storage[key]
	context.mutex.Unlock()

	if !ok {
		return NullBulkString{}
	}
	if !entity.expireAt.IsZero() && entity.expireAt.Before(time.Now()) {
		return NullBulkString{}
	} else {
		return BulkString{entity.value}
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
