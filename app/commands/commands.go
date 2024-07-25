package commands

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/args"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Command func([]resp.RespDataType, resp.RespDataType, io.Writer, *Context) error
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
	"ping":     ping,
	"echo":     echo,
	"set":      set,
	"get":      get,
	"replconf": replconf,
	"psync":    psync,
	"info":     info,
	"wait":     wait,
	"config":   config,
	"keys":     keys,
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

func ping(args []resp.RespDataType, _ resp.RespDataType, writer io.Writer, _ *Context) error {
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

func echo(args []resp.RespDataType, _ resp.RespDataType, writer io.Writer, _ *Context) error {
	if len(args) != 1 {
		return fmt.Errorf("ECHO command has 1 argument")
	}
	_, err := writer.Write(args[0].Bytes())
	return err
}

func set(args []resp.RespDataType, request resp.RespDataType, writer io.Writer, context *Context) error {
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

	master, ok := context.ReplicationRole.(*replication.MasterRole)
	if ok {
		master.Propagate(request)
	}

	_, err := writer.Write(SimpleString("OK").Bytes())
	return err
}

func get(args []resp.RespDataType, _ resp.RespDataType, writer io.Writer, context *Context) error {
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

func replconf(args []resp.RespDataType, _ resp.RespDataType, writer io.Writer, context *Context) error {
	if len(args) == 0 {
		return fmt.Errorf("replconf must contain arguments")
	}
	command := args[0].(BulkString)
	if command == "getack" {
		conn := writer.(*replication.SlaveConnection)
		return conn.Ack()
	} else if command == "ack" && len(args) == 2 {
		offset, err := strconv.ParseUint(string(args[1].(BulkString)), 10, 64)
		if err != nil {
			return err
		}
		master := context.ReplicationRole.(*replication.MasterRole)
		master.AckReceived(offset)
		return nil
	} else {
		_, err := writer.Write(SimpleString("OK").Bytes())
		return err
	}
}

func psync(_ []resp.RespDataType, _ resp.RespDataType, writer io.Writer, context *Context) error {
	master := context.ReplicationRole.(*replication.MasterRole)
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
	if err == nil {
		master.AddReplica(writer.(net.Conn))
	}
	return err
}

func info(_ []resp.RespDataType, _ resp.RespDataType, writer io.Writer, context *Context) error {
	_, err := writer.Write(replicationInfo(context).Bytes())
	return err
}

func wait(args []resp.RespDataType, _ resp.RespDataType, writer io.Writer, context *Context) error {
	if len(args) != 2 {
		return fmt.Errorf("numreplicas and timeout must be specified")
	}
	numOfReplicas, err := strconv.Atoi(string(args[0].(BulkString)))
	if err != nil {
		return err
	}
	timeout, err := strconv.Atoi(string(args[1].(BulkString)))
	if err != nil {
		return err
	}
	master := context.ReplicationRole.(*replication.MasterRole)
	numOfReplicas = master.Wait(numOfReplicas, time.Duration(timeout)*time.Millisecond)
	_, err = writer.Write(resp.Integer(numOfReplicas).Bytes())
	return err
}

func config(args []resp.RespDataType, _ resp.RespDataType, writer io.Writer, context *Context) error {
	if len(args) < 2 {
		return fmt.Errorf("parameters must be specified")
	}
	if args[0].(BulkString) != "get" {
		return nil
	}
	response := make([]resp.RespDataType, 0, len(args[1:])*2)
	for _, parameter := range args[1:] {
		key := parameter.(BulkString)
		value, ok := context.args.Raw[string(key)]
		if ok {
			response = append(response, key, BulkString(value))
		}
	}
	_, err := writer.Write(resp.Array{Content: response}.Bytes())
	return err
}

func keys(args []resp.RespDataType, _ resp.RespDataType, writer io.Writer, context *Context) error {
	if len(args) == 0 {
		return fmt.Errorf("pattern mus be specified")
	}
	if args[0].(BulkString) != "*" {
		return fmt.Errorf("only * wildcard supported for now")
	}
	fullPath := filepath.Join(context.args.RdbDir, context.args.RdbFileName)
	file, err := os.Open(fullPath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	strategy := &accumulateKeysReadStrategy{}
	err = rdb.Read(reader, strategy)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return err
	}
	_, err = writer.Write(resp.Array{Content: strategy.buf}.Bytes())
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

type accumulateKeysReadStrategy struct {
	buf []resp.RespDataType
}

func (s *accumulateKeysReadStrategy) AddDbEntry(entry rdb.DbEntry) {
	s.buf = append(s.buf, entry.Key)
}

func (*accumulateKeysReadStrategy) AddAux(_ string, _ resp.RespDataType) {}
