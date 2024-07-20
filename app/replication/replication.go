package replication

import (
	"math/rand"
	"strconv"
	"strings"
)

type Role interface {
	CollectInfo(map[string]string)
}

type MasterRole struct {
	id     string
	offset uint64
}

type SlaveRole struct{
	Address ReplicaAddress
}

type ReplicaAddress struct {
	Host string
	Port uint16
}

func NewMaster() MasterRole {
	return MasterRole{
		id:     generateRepId(),
		offset: 0,
	}
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
