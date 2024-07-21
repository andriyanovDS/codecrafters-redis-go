package replication

import (
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
	id     string
	offset uint64
}

func NewMaster() MasterRole {
	return MasterRole{
		id:     generateRepId(),
		offset: 0,
	}
}

type SlaveRole struct {
	Address ReplicaAddress
}

type ReplicaAddress struct {
	Host string
	Port uint16
}

type Connection interface {
	Handshake()
}

type SlaveConnection struct {
	conn net.Conn
}

func ConnectToMaster(slave SlaveRole) (Connection, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%d", slave.Address.Host, slave.Address.Port))
	if err != nil {
		return nil, err
	}
	return SlaveConnection{
		conn: conn,
	}, nil
}

func (c SlaveConnection) Handshake() {
	ping := resp.Array{
		Content: []resp.RespDataType{resp.BulkString("Ping")},
	}
	c.conn.Write(ping.Bytes())
}

func (r MasterRole) CollectInfo(info map[string]string) {
	info["role"] = "master"
	info["master_replid"] = r.id
	info["master_repl_offset"] = strconv.FormatUint(r.offset, 10)
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
