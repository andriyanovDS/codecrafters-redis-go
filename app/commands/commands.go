package commands

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/args"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Command func(resp.Array, *Context) resp.RespDataType
type BulkString = resp.BulkString
type SimpleString = resp.SimpleString
type NullBulkString = resp.NullBulkString

type Context struct {
	args            args.Args
	storage         map[string]entity
	replicationRole replication.Role
	mutex           sync.Mutex
}

type entity struct {
	value    string
	expireAt time.Time
}

func NewContext(args args.Args) Context {
	return Context{
		args:    args,
		storage: make(map[string]entity),
		replicationRole: func() replication.Role {
			if args.ReplicaOf.Host != "" {
				return replication.SlaveRole{
					Address: args.ReplicaOf,
				}
			} else {
				return replication.NewMaster()
			}
		}(),
		mutex: sync.Mutex{},
	}
}

func PingCommand(resp resp.Array, _ *Context) resp.RespDataType {
	len := len(resp.Content)
	if len == 0 || len > 2 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(string(command)) != "ping" {
		return nil
	}
	if len == 2 {
		return resp.Content[1]
	} else {
		return BulkString("PONG")
	}
}

func EchoCommand(resp resp.Array, _ *Context) resp.RespDataType {
	if len(resp.Content) != 2 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(string(command)) != "echo" {
		return nil
	}
	return resp.Content[1]
}

func SetCommand(resp resp.Array, context *Context) resp.RespDataType {
	if len(resp.Content) == 0 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(string(command)) != "set" {
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
	return SimpleString("OK")
}

func GetCommand(resp resp.Array, context *Context) resp.RespDataType {
	if len(resp.Content) != 2 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(string(command)) != "get" {
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
		return BulkString(entity.value)
	}
}

func InfoCommand(resp resp.Array, context *Context) resp.RespDataType {
	if len(resp.Content) == 0 {
		return nil
	}
	command := resp.Content[0].(BulkString)
	if strings.ToLower(string(command)) != "info" {
		return nil
	}
	return replicationInfo(context)
}

func replicationInfo(context *Context) BulkString {
	var builder strings.Builder
	builder.WriteString("# Replication\r\n")
	info := make(map[string]string)
	context.replicationRole.CollectInfo(info)

	for key, value := range info {
		builder.WriteString(key)
		builder.WriteByte(':')
		builder.WriteString(value)
		builder.WriteByte('\r')
		builder.WriteByte('\n')
	}
	return resp.BulkString(builder.String())
}

func toString(r resp.RespDataType) string {
	switch t := r.(type) {
	case BulkString:
		return string(t)
	case resp.SimpleString:
		return string(t)
	default:
		return ""
	}
}
