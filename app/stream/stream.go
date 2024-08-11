package stream

import (
	"bytes"
	"fmt"
	"slices"
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
	root   node
	lastID streamID
	len    uint64
}

type RangeMatch struct {
	Id   string
	Pair []Pair
}

func New(id string, payload []Pair) (*Stream, error) {
	streamID, err := parseID(id, streamID{})
	if err != nil {
		return nil, err
	}
	if streamID.ms == 0 && streamID.sequence == 0 {
		return nil, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	n := node{
		leaf:   &entry{id: streamID, payload: payload},
		prefix: []byte(id),
	}
	root := node{
		edges: []*node{&n},
	}
	return &Stream{root: root, lastID: streamID, len: 1}, nil
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
	id = streamID.String()
	s.lastID = streamID
	s.root.insert([]byte(id), streamID, payload)
	return streamID.String(), nil
}

func (s *Stream) Read(id string) []RangeMatch {
	matches := make([]RangeMatch, 0)
	search := []byte(id)
	node := s.root
	for _, edge := range node.edges {
		if bytes.Compare(edge.prefix, search) < 1 {
			continue
		}
		edge.rangeMin(search, &matches)
	}
	return matches
}

func (s *Stream) Range(start string, end string) []RangeMatch {
	matches := make([]RangeMatch, 0)
	minId := []byte(start)
	maxId := []byte(end)
	node := s.root
	prefix := node.prefix
	for {
		if minId[0] != maxId[0] {
			break
		}
		if len(prefix) > 0 {
			if prefix[0] == minId[0] {
				minId = minId[1:]
				maxId = maxId[1:]
				prefix = prefix[1:]
				continue
			} else {
				break
			}
		}
		_, child := node.child(minId[0])
		if child != nil {
			node = *child
			prefix = child.prefix
		} else {
			break
		}
	}
	for _, edge := range node.edges {
		first := edge.prefix[0]
		if start != "-" && first < minId[0] {
			continue
		} else if start != "-" && first == minId[0] {
			edge.rangeMin(minId, &matches)
		} else if end != "+" && first == maxId[0] {
			edge.rangeMax(maxId, &matches)
		} else if end != "+" && first > maxId[0] {
			break
		} else {
			edge.traversee(&matches)
		}
	}
	return matches
}

func (n *node) rangeMin(min []byte, matches *[]RangeMatch) {
	if n.leaf != nil && bytes.Compare(n.prefix, min) >= 0 {
		*matches = append(*matches, RangeMatch{
			Id:   n.leaf.id.String(),
			Pair: n.leaf.payload,
		})
		return
	}
	min = min[suffixIdx(n.prefix, min):]
	for _, edge := range n.edges {
		first := edge.prefix[0]
		if first < min[0] {
			continue
		}
		if first > min[0] {
			edge.traversee(matches)
		} else {
			edge.rangeMin(min, matches)
		}
	}
}

func (n *node) rangeMax(max []byte, matches *[]RangeMatch) {
	if n.leaf != nil && bytes.Compare(n.prefix, max) < 1 {
		*matches = append(*matches, RangeMatch{
			Id:   n.leaf.id.String(),
			Pair: n.leaf.payload,
		})
		return
	}
	max = max[suffixIdx(n.prefix, max):]
	for _, edge := range n.edges {
		first := edge.prefix[0]
		if first > max[0] {
			return
		}
		if first < max[0] {
			edge.traversee(matches)
		} else {
			edge.rangeMax(max, matches)
		}
	}
}

func (n *node) traversee(matches *[]RangeMatch) {
	if n.leaf != nil {
		*matches = append(*matches, RangeMatch{
			Id:   n.leaf.id.String(),
			Pair: n.leaf.payload,
		})
		return
	}
	for _, edge := range n.edges {
		edge.traversee(matches)
	}
}

func (n *node) insert(search []byte, id streamID, payload []Pair) {
	childIndex, child := n.child(search[0])

	if child == nil {
		n.appendEdge(&node{
			leaf:   &entry{id: id, payload: payload},
			prefix: search,
		})
		return
	}

	suffixIdx := suffixIdx(child.prefix, search)
	suffix := search[suffixIdx:]
	prefix := search[0:suffixIdx]

	if len(prefix) == len(child.prefix) {
		child.insert(suffix, id, payload)
	} else {
		splitted := &node{
			prefix: prefix,
			edges:  make([]*node, 0),
		}
		child.prefix = child.prefix[suffixIdx:]
		splitted.appendEdge(child)
		splitted.appendEdge(&node{
			leaf:   &entry{id: id, payload: payload},
			prefix: suffix,
		})
		n.edges[childIndex] = splitted
	}
}

func (n *node) appendEdge(edge *node) {
	for index, node := range n.edges {
		if node.prefix[0] > edge.prefix[0] {
			n.edges = slices.Insert(n.edges, index, edge)
			return
		}
	}
	n.edges = append(n.edges, edge)
}

func (s *Stream) LastID() string {
	return s.lastID.String()
}

func (n *node) child(prefix byte) (int, *node) {
	for i, edge := range n.edges {
		if edge.prefix[0] == prefix {
			return i, edge
		}
	}
	return 0, nil
}

func suffixIdx(prefix []byte, search []byte) int {
	maxLen := min(len(prefix), len(search))
	start := 0
	for i := 0; i < maxLen; i++ {
		if prefix[i] != search[i] {
			break
		}
		start += 1
	}
	return start
}

func parseID(id string, lastID streamID) (streamID, error) {
	parts := strings.Split(id, "-")
	if len(parts) == 0 {
		return streamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
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
