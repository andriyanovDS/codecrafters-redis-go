package replication

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Role interface {
	CollectInfo(map[string]string)
}

type MasterRole struct {
	Id     string
	Offset uint64
}

func NewMaster() MasterRole {
	return MasterRole{
		Id:     generateRepId(),
		Offset: 0,
	}
}

type SlaveRole struct {
	Address      ReplicaAddress
	masterReplId string
	offset       uint64
}

type ReplicaAddress struct {
	Host string
	Port uint16
}

type Connection interface {
	Handshake(port uint16) error
}

type SlaveConnection struct {
	conn  net.Conn
	slave SlaveRole
}

func ConnectToMaster(slave SlaveRole) (Connection, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%d", slave.Address.Host, slave.Address.Port))
	if err != nil {
		return nil, err
	}
	return SlaveConnection{
		conn:  conn,
		slave: slave,
	}, nil
}

func (c SlaveConnection) Handshake(port uint16) error {
	ping := resp.Array{
		Content: []resp.RespDataType{resp.BulkString("ping")},
	}
	_, err := c.conn.Write(ping.Bytes())
	if err != nil {
		return err
	}
	reader := bufio.NewReader(c.conn)
	response, err := resp.Parse(reader)
	if err != nil {
		return err
	}
	command := response.(resp.SimpleString)
	if command != "pong" {
		return fmt.Errorf("unexpected response: %v", command)
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
		return err
	}
	response, err = resp.Parse(reader)
	if err != nil {
		return err
	}
	command = response.(resp.SimpleString)
	if command != "ok" {
		return fmt.Errorf("unexpected response: %v", command)
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
		return err
	}
	response, err = resp.Parse(reader)
	if err != nil {
		return err
	}
	command = response.(resp.SimpleString)
	if command != "ok" {
		return fmt.Errorf("unexpected response: %v", command)
	}

	_, err = c.conn.Write(c.slave.Psync().Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (r MasterRole) CollectInfo(info map[string]string) {
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
	if s.offset == 0 {
		offset = -1
	} else {
		offset = int(s.offset)
	}
	return resp.Array{
		Content: []resp.RespDataType{
			resp.BulkString("PSYNC"),
			resp.BulkString(masterReplId),
			resp.BulkString(strconv.Itoa(offset)),
		},
	}
}