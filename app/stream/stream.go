package stream

import (
	"fmt"
	"strconv"
	"strings"
)

type streamID struct {
	ms       uint64
	sequence uint64
}

type Pair struct {
	Field string
	Value string
}

type entry struct {
	id      streamID
	payload []Pair
}

type node struct {
	leaf   *entry
	edges  []*node
	prefix []byte
}

type Stream struct {
	root   *node
	lastID streamID
	len    uint64
}

func New(id string, payload []Pair) (*Stream, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Stream ID must be in format <millisecondsTime>-<sequenceNumber>")
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	streamID := streamID{ms: ms, sequence: seq}
	node := node{
		leaf:   &entry{id: streamID, payload: payload},
		prefix: []byte(id),
	}
	return &Stream{root: &node, lastID: streamID, len: 1}, nil
}
