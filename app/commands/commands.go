package commands

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/args"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Command func([]resp.RespDataType, io.Writer, *Context) error
type BulkString = resp.BulkString
type SimpleString = resp.SimpleString
type NullBulkString = resp.NullBulkString

type Context struct {
	args            args.Args
	storage         map[string]entity
	ReplicationRole replication.Role
	mutex           sync.Mutex
}

type entity struct {
	value    string
	expireAt time.Time
}

var Commands = map[string]Command{
	"ping":     Ping,
	"echo":     Echo,
	"set":      Set,
	"get":      Get,
	"replconf": Replconf,
	"psync":    Psync,
	"info":     Info,
}

func NewContext(args args.Args) Context {
	return Context{
		args:    args,
		storage: make(map[string]entity),
		ReplicationRole: func() replication.Role {
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

func Ping(args []resp.RespDataType, writer io.Writer, _ *Context) error {
	len := len(args)
	var response resp.RespDataType
	if len == 1 {
		response = args[1]
	} else {
		response = BulkString("PONG")
	}
	_, err := writer.Write(response.Bytes())
	return err
}

func Echo(args []resp.RespDataType, writer io.Writer, _ *Context) error {
	if len(args) != 1 {
		return fmt.Errorf("ECHO command has 1 argument")
	}
	_, err := writer.Write(args[0].Bytes())
	return err
}

func Set(args []resp.RespDataType, writer io.Writer, context *Context) error {
	if len(args) < 2 {
		return fmt.Errorf("SET command has at least 2 arguments")
	}
	key := resp.String(args[0])
	entity := entity{
		value:    resp.String(args[1]),
		expireAt: time.Time{},
	}

	var argIndex = 2
	for {
		if argIndex >= len(args) {
			break
		}
		func() {
			defer func() { argIndex += 2 }()
			arg := resp.String(args[argIndex])
			if strings.ToLower(arg) == "px" {
				ms, err := strconv.Atoi(resp.String(args[argIndex+1]))
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
	_, err := writer.Write(SimpleString("OK").Bytes())
	return err
}

func Get(args []resp.RespDataType, writer io.Writer, context *Context) error {
	if len(args) != 1 {
		return fmt.Errorf("GET command has 1 argument")
	}
	key := resp.String(args[0])

	context.mutex.Lock()
	entity, ok := context.storage[key]
	context.mutex.Unlock()

	var resp resp.RespDataType
	if !ok {
		resp = NullBulkString{}
	} else if !entity.expireAt.IsZero() && entity.expireAt.Before(time.Now()) {
		resp = NullBulkString{}
	} else {
		resp = BulkString(entity.value)
	}
	_, err := writer.Write(resp.Bytes())
	return err
}

func Replconf(_ []resp.RespDataType, writer io.Writer, _ *Context) error {
	_, err := writer.Write(SimpleString("OK").Bytes())
	return err
}

func Psync(_ []resp.RespDataType, writer io.Writer, context *Context) error {
	master := context.ReplicationRole.(replication.MasterRole)
	response := fmt.Sprintf("FULLRESYNC %v %d", master.Id, master.Offset)
	_, err := writer.Write(SimpleString(response).Bytes())
	if err != nil {
		return err
	}
	emptyRdp, err := rdb.Empty()
	if err != nil {
		return err
	}
	syncResp := BulkString(string(emptyRdp)).Bytes()
	_, err = writer.Write(syncResp[:len(syncResp)-2])
	return err
}

func Info(_ []resp.RespDataType, writer io.Writer, context *Context) error {
	_, err := writer.Write(replicationInfo(context).Bytes())
	return err
}

func replicationInfo(context *Context) BulkString {
	var builder strings.Builder
	builder.WriteString("# Replication\r\n")
	info := make(map[string]string)
	context.ReplicationRole.CollectInfo(info)

	for key, value := range info {
		builder.WriteString(key)
		builder.WriteByte(':')
		builder.WriteString(value)
		builder.WriteByte('\r')
		builder.WriteByte('\n')
	}
	return resp.BulkString(builder.String())
}
