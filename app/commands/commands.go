package commands

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/args"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/stream"
)

type command func([]resp.RespDataType, resp.RespDataType, writer, *Context) error
type transactionCommand func(started bool, key string, w writer, c *Context) error
type BulkString = resp.BulkString
type SimpleString = resp.SimpleString
type NullBulkString = resp.NullBulkString

type writer interface {
	Write(resp.RespDataType) error
}

type connectionWriter struct {
	conn io.Writer
}

func (c connectionWriter) Write(r resp.RespDataType) error {
	_, err := c.conn.Write(r.Bytes())
	return err
}

type execWriter struct {
	buf []resp.RespDataType
}

func (c *execWriter) Write(r resp.RespDataType) error {
	c.buf = append(c.buf, r)
	return nil
}

type Context struct {
	args            args.Args
	storage         map[string]entity
	queue           map[string][]resp.RespDataType
	ReplicationRole replication.Role
	mutex           sync.Mutex
}

type entity struct {
	value    interface{}
	expireAt time.Time
}

func (c *Context) RdbFilePath() string {
	return filepath.Join(c.args.RdbDir, c.args.RdbFileName)
}

func (c *Context) AddEntity(e rdb.DbEntry) {
	c.storage[string(e.Key)] = entity{
		value:    resp.String(e.Value),
		expireAt: e.ExpireAt,
	}
}

var commands = map[string]command{
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
	"incr":     incr,
	"type":     type_,
	"xadd":     xadd,
}

var transactionCommands = map[string]transactionCommand{
	"multi":   multi,
	"exec":    exec,
	"discard": discard,
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
		queue: make(map[string][]resp.RespDataType),
		mutex: sync.Mutex{},
	}
}

func Handle(req resp.RespDataType, writer io.Writer, context *Context) {
	w := connectionWriter{conn: writer}
	conn, ok := writer.(net.Conn)
	if !ok {
		handle(req, w, context)
		return
	}
	queueKey := conn.RemoteAddr().String()
	queue, transactionStarted := context.queue[queueKey]
	request, ok := req.(resp.Array)
	if !ok {
		fmt.Printf("ignored command: %v\n", req)
		return
	}
	if len(request.Content) == 0 {
		return
	}
	command := string(request.Content[0].(BulkString))
	handler, ok := transactionCommands[command]
	if ok {
		handler(transactionStarted, queueKey, w, context)
	} else if transactionStarted {
		context.queue[queueKey] = append(queue, req)
		_ = w.Write(resp.SimpleString("QUEUED"))
	} else {
		handle(req, w, context)
	}
}

func handle(req resp.RespDataType, writer writer, context *Context) {
	request, ok := req.(resp.Array)
	if !ok {
		fmt.Printf("ignored command: %v\n", req)
		return
	}
	if len(request.Content) == 0 {
		return
	}
	command := string(request.Content[0].(BulkString))
	fmt.Printf("Command executed %s\n", command)
	handler, ok := commands[strings.ToLower(command)]
	if !ok {
		fmt.Printf("unknown command received: %v\n", command)
		return
	}
	err := handler(request.Content[1:], req, writer, context)
	if err != nil {
		fmt.Printf("%s command handling failure: %v\n", command, err)
	}
}

func ping(args []resp.RespDataType, _ resp.RespDataType, writer writer, _ *Context) error {
	len := len(args)
	var response resp.RespDataType
	if len == 1 {
		response = args[1]
	} else {
		response = BulkString("PONG")
	}
	return writer.Write(response)
}

func echo(args []resp.RespDataType, _ resp.RespDataType, writer writer, _ *Context) error {
	if len(args) != 1 {
		return fmt.Errorf("ECHO command has 1 argument")
	}
	return writer.Write(args[0])
}

func set(args []resp.RespDataType, request resp.RespDataType, writer writer, context *Context) error {
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
	return writer.Write(SimpleString("OK"))
}

func get(args []resp.RespDataType, req resp.RespDataType, writer writer, context *Context) error {
	if len(args) != 1 {
		return fmt.Errorf("GET command has 1 argument")
	}
	key := resp.String(args[0])

	context.mutex.Lock()
	entity, ok := context.storage[key]
	context.mutex.Unlock()

	var response resp.RespDataType
	if !ok {
		response = NullBulkString{}
	} else if !entity.expireAt.IsZero() && entity.expireAt.Before(time.Now()) {
		response = NullBulkString{}
	} else {
		response = resp.BulkString(entity.value.(string))
	}
	return writer.Write(response)
}

func incr(args []resp.RespDataType, req resp.RespDataType, writer writer, context *Context) error {
	if len(args) != 1 {
		return fmt.Errorf("INCR command has 1 argument")
	}
	key := resp.String(args[0])

	var value int
	context.mutex.Lock()
	defer context.mutex.Unlock()
	e, ok := context.storage[key]
	if !ok || (!e.expireAt.IsZero() && e.expireAt.Before(time.Now())) {
		e.expireAt = time.Time{}
		value = 1
	} else {
		intVal, err := strconv.Atoi(e.value.(string))
		if err != nil {
			return writer.Write(resp.Error("ERR value is not an integer or out of range"))
		}
		value = intVal + 1
	}
	context.storage[key] = entity{
		value:    strconv.Itoa(value),
		expireAt: e.expireAt,
	}
	return writer.Write(resp.Integer(value))
}

func type_(args []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
	if len(args) != 1 {
		return fmt.Errorf("GET command has 1 argument")
	}
	key := resp.String(args[0])

	context.mutex.Lock()
	entity, ok := context.storage[key]
	context.mutex.Unlock()

	var response resp.RespDataType
	if !ok {
		response = resp.SimpleString("none")
	} else if !entity.expireAt.IsZero() && entity.expireAt.Before(time.Now()) {
		response = resp.SimpleString("none")
	} else {
		switch entity.value.(type) {
		case string:
			response = resp.SimpleString("string")
		case *stream.Stream:
			response = resp.SimpleString("stream")
		default:
			return fmt.Errorf("unexpected entity type: %T", entity.value)
		}
	}
	return writer.Write(response)
}

func xadd(args []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
	if len(args) < 2 {
		return fmt.Errorf("XADD command must contain key and id")
	}
	key := resp.String(args[0].(BulkString))
	id := resp.String(args[1].(BulkString))
	var payload []stream.Pair
	for i := 2; i < len(args); i += 2 {
		if i+1 == len(args) {
			return fmt.Errorf("value is missing")
		}
		payload = append(payload, stream.Pair{
			Field: resp.String(args[i].(BulkString)),
			Value: resp.String(args[i+1].(BulkString)),
		})
	}
	context.mutex.Lock()
	_, ok := context.storage[key]
	if ok {
		return fmt.Errorf("append to stream not supported yet")
	} else {
		stream, err := stream.New(id, payload)
		if err != nil {
			return err
		}
		context.storage[key] = entity{
			value: stream,
		}
	}
	context.mutex.Unlock()
	return writer.Write(resp.SimpleString(id))
}

func replconf(args []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
	if len(args) == 0 {
		return fmt.Errorf("replconf must contain arguments")
	}
	command := args[0].(BulkString)
	if command == "getack" {
		conn := writer.(connectionWriter).conn.(*replication.SlaveConnection)
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
		return writer.Write(SimpleString("OK"))
	}
}

func psync(_ []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
	master := context.ReplicationRole.(*replication.MasterRole)
	response := fmt.Sprintf("FULLRESYNC %v %d", master.Id, master.Offset)
	err := writer.Write(SimpleString(response))
	if err != nil {
		return err
	}
	emptyRdp, err := rdb.Empty()
	if err != nil {
		return err
	}
	err = writer.Write(resp.RdbString(string(emptyRdp)))
	if err == nil {
		master.AddReplica(writer.(connectionWriter).conn.(net.Conn))
	}
	return err
}

func info(_ []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
	return writer.Write(replicationInfo(context))
}

func wait(args []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
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
	return writer.Write(resp.Integer(numOfReplicas))
}

func config(args []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
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
	return writer.Write(resp.Array{Content: response})
}

func keys(args []resp.RespDataType, _ resp.RespDataType, writer writer, context *Context) error {
	if len(args) == 0 {
		return fmt.Errorf("pattern mus be specified")
	}
	if args[0].(BulkString) != "*" {
		return fmt.Errorf("only * wildcard supported for now")
	}
	context.mutex.Lock()
	keys := make([]resp.RespDataType, 0)
	for key := range context.storage {
		keys = append(keys, resp.BulkString(key))
	}
	context.mutex.Unlock()
	return writer.Write(resp.Array{Content: keys})
}

func multi(_ bool, key string, w writer, c *Context) error {
	c.queue[key] = make([]resp.RespDataType, 0)
	return w.Write(resp.SimpleString("OK"))
}

func exec(started bool, key string, w writer, c *Context) error {
	if !started {
		return w.Write(resp.Error("ERR EXEC without MULTI"))
	}
	queue := c.queue[key]
	delete(c.queue, key)
	writer := execWriter{
		buf: make([]resp.RespDataType, 0, len(c.queue)),
	}
	for _, comm := range queue {
		handle(comm, &writer, c)
	}
	return w.Write(resp.Array{Content: writer.buf})
}

func discard(started bool, key string, w writer, c *Context) error {
	if started {
		delete(c.queue, key)
		return w.Write(resp.SimpleString("OK"))
	} else {
		return w.Write(resp.Error("ERR DISCARD without MULTI"))
	}
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
