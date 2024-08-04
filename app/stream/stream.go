package stream

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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
	streamID, err := parseID(id, streamID{})
	if err != nil {
		return nil, err
	}
	if streamID.ms == 0 && streamID.sequence == 0 {
		return nil, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	node := node{
		leaf:   &entry{id: streamID, payload: payload},
		prefix: []byte(id),
	}
	return &Stream{root: &node, lastID: streamID, len: 1}, nil
}

func (s *Stream) Insert(id string, payload []Pair) (string, error) {
	streamID, err := parseID(id, s.lastID)
	if err != nil {
		return "", err
	}
	if streamID.ms == 0 && streamID.sequence == 0 {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	if streamID.ms < s.lastID.ms {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	if streamID.ms == s.lastID.ms && streamID.sequence <= s.lastID.sequence {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	s.lastID = streamID
	return streamID.String(), nil
}

func (s *Stream) LastID() string {
	return s.lastID.String()
}

func parseID(id string, lastID streamID) (streamID, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return streamID{}, fmt.Errorf("Stream ID must be in format <millisecondsTime>-<sequenceNumber>")
	}
	if parts[0] == "*" {
		ms := uint64(time.Now().UnixMilli())
		if ms == lastID.ms {
			return streamID{ms: ms, sequence: lastID.sequence + 1}, nil
		} else {
			return streamID{ms: ms, sequence: 0}, nil
		}
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return streamID{}, err
	}
	if parts[1] == "*" {
		if lastID.ms == ms {
			return streamID{ms: ms, sequence: lastID.sequence + 1}, nil
		} else {
			return streamID{ms: ms, sequence: 0}, nil
		}
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return streamID{}, err
	}
	return streamID{ms: ms, sequence: seq}, nil
}

func (id streamID) String() string {
	return fmt.Sprintf("%d-%d", id.ms, id.sequence)
}
