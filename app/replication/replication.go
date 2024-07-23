package replication

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Role interface {
	CollectInfo(map[string]string)
}

type MasterRole struct {
	Id       string
	Offset   uint64
	mutex    sync.Mutex
	replicas []net.Conn
}

func (m *MasterRole) AddReplica(conn net.Conn) {
	m.mutex.Lock()
	m.replicas = append(m.replicas, conn)
	m.mutex.Unlock()
}

func (m *MasterRole) Propagate(request resp.RespDataType) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.replicas) == 0 {
		return
	}
	bytes := request.Bytes()
	for _, conn := range m.replicas {
		go conn.Write(bytes)
	}
}

func NewMaster() *MasterRole {
	return &MasterRole{
		Id:     generateRepId(),
		Offset: 0,
	}
}

type SlaveRole struct {
	Address      ReplicaAddress
	masterReplId string
	Offset       uint64
}

type ReplicaAddress struct {
	Host string
	Port uint16
}

type Connection interface {
	io.Reader
	io.Writer
	Handshake(port uint16) (*bufio.Reader, error)
	Ack() error
}

type SlaveConnection struct {
	conn   net.Conn
	reader *slaveReader
	slave  *SlaveRole
}

type slaveReader struct {
	reader             io.Reader
	handshakeCompleted bool
	bytesRead          uint64
}

func (r *slaveReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	if r.handshakeCompleted {
		r.bytesRead += uint64(n)
	}
	return n, err
}

func (r *slaveReader) markHandshakeAsCompleted(offset uint64) {
	r.handshakeCompleted = true
	r.bytesRead = offset
}

func ConnectToMaster(slave *SlaveRole) (Connection, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%d", slave.Address.Host, slave.Address.Port))
	if err != nil {
		return nil, err
	}
	return &SlaveConnection{
		conn:   conn,
		reader: &slaveReader{reader: conn},
		slave:  slave,
	}, nil
}

func (c *SlaveConnection) Handshake(port uint16) (*bufio.Reader, error) {
	ping := resp.Array{
		Content: []resp.RespDataType{resp.BulkString("ping")},
	}
	_, err := c.conn.Write(ping.Bytes())
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(c.reader)
	response, err := resp.Parse(reader)
	if err != nil {
		return nil, err
	}
	command := response.(resp.SimpleString)
	if command != "pong" {
		return nil, fmt.Errorf("unexpected response: %v", command)
	}

	lisneningPort := resp.Array{
		Content: []resp.RespDataType{
			resp.BulkString("REPLCONF"),
			resp.BulkString("listening-port"),
			resp.BulkString(strconv.Itoa(int(port))),
		},
	}
	_, err = c.conn.Write(lisneningPort.Bytes())
	if err != nil {
		return nil, err
	}
	response, err = resp.Parse(reader)
	if err != nil {
		return nil, err
	}
	command = response.(resp.SimpleString)
	if command != "ok" {
		return nil, fmt.Errorf("unexpected response: %v", command)
	}

	capabilities := resp.Array{
		Content: []resp.RespDataType{
			resp.BulkString("REPLCONF"),
			resp.BulkString("capa"),
			resp.BulkString("psync2"),
		},
	}
	_, err = c.conn.Write(capabilities.Bytes())
	if err != nil {
		return nil, err
	}
	response, err = resp.Parse(reader)
	if err != nil {
		return nil, err
	}
	command = response.(resp.SimpleString)
	if command != "ok" {
		return nil, fmt.Errorf("unexpected response: %v", command)
	}

	_, err = c.conn.Write(c.slave.Psync().Bytes())
	if err != nil {
		return nil, err
	}

	response, err = resp.Parse(reader)
	if err != nil {
		return nil, err
	}
	command = response.(resp.SimpleString)
	if strings.HasPrefix(string(command), "FULLRESYNC") {
		return nil, fmt.Errorf("unexpected response: %v", command)
	}

	response, err = resp.Parse(reader)
	if err != nil {
		return nil, err
	}
	rdpCommand := response.(resp.BulkString)
	if strings.HasPrefix(string(rdpCommand), "REDIS") {
		return nil, fmt.Errorf("unexpected response: %v", command)
	}
	c.reader.markHandshakeAsCompleted(uint64(reader.Buffered()))
	return reader, nil
}

func (c *SlaveConnection) Read(p []byte) (n int, err error) {
	return c.reader.Read(p)
}

func (c *SlaveConnection) Write(_ []byte) (int, error) {
	c.slave.Offset = c.reader.bytesRead
	return 0, nil
}

func (c *SlaveConnection) Ack() error {
	response := resp.Array{
		Content: []resp.RespDataType{
			resp.BulkString("REPLCONF"),
			resp.BulkString("ACK"),
			resp.BulkString(strconv.Itoa(int(c.slave.Offset))),
		},
	}
	_, err := c.conn.Write(response.Bytes())
	return err
}

func (r *MasterRole) CollectInfo(info map[string]string) {
	info["role"] = "master"
	info["master_replid"] = r.Id
	info["master_repl_offset"] = strconv.FormatUint(r.Offset, 10)
}

func (r SlaveRole) CollectInfo(info map[string]string) {
	info["role"] = "slave"
}

func generateRepId() string {
	var builder strings.Builder
	var letters = []rune("0123456789abcdefghijklmnopqrstuvwxyz")
	for range 40 {
		letter := letters[rand.Intn(len(letters))]
		builder.WriteRune(letter)
	}
	return builder.String()
}

func (s SlaveRole) Psync() resp.RespDataType {
	var masterReplId string
	if s.masterReplId != "" {
		masterReplId = s.masterReplId
	} else {
		masterReplId = "?"
	}
	var offset int
	if s.Offset == 0 {
		offset = -1
	} else {
		offset = int(s.Offset)
	}
	return resp.Array{
		Content: []resp.RespDataType{
			resp.BulkString("PSYNC"),
			resp.BulkString(masterReplId),
			resp.BulkString(strconv.Itoa(offset)),
		},
	}
}
