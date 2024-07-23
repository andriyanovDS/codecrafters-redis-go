package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	IntegerByte      = ':'
	SimpleStringByte = '+'
	ErrorByte        = '-'
	ArrayByte        = '*'
	BulkStringByte   = '$'
)

type RespDataType interface {
	Bytes() []byte
}

type Array struct {
	Content []RespDataType
}

func (a Array) Command() string {
	if len(a.Content) == 0 {
		return ""
	}
	return strings.ToLower(String(a.Content[0]))
}

type BulkString string
type SimpleString string
type Integer int64
type NullBulkString struct{}

func Parse(reader *bufio.Reader) (RespDataType, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		fmt.Printf("failed to read byte: %v", err)
		return nil, err
	}
	switch firstByte {
	case BulkStringByte:
		length, err := readInt(reader)
		if err != nil {
			return nil, err
		}
		bytes, err := readExact(reader, int(length))
		if err != nil {
			fmt.Printf("failed to read bulk string: %v\n", err)
			return nil, err
		}
		return BulkString(strings.ToLower(string(bytes))), nil
	case ArrayByte:
		length, err := readInt(reader)
		if err != nil {
			return nil, err
		}
		elements := make([]RespDataType, 0, length)
		for i := int64(0); i < length; i++ {
			nextEl, err := Parse(reader)
			if err != nil {
				fmt.Printf("failed to read array element: %v\n", err)
				return nil, err
			}
			elements = append(elements, nextEl)
		}
		return Array{Content: elements}, nil
	case IntegerByte:
		sign, err := reader.ReadByte()
		if err != nil {
			fmt.Printf("failed to read integer: %v\n", err)
			return nil, err
		}
		var isNegative bool
		switch sign {
		case '-':
			isNegative = true
		case '+':
			isNegative = false
		default:
			reader.UnreadByte()
		}
		integer, err := readInt(reader)
		if err != nil {
			fmt.Printf("failed to read integer: %v\n", err)
			return nil, err
		}
		if isNegative {
			integer = -integer
		}
		return Integer(integer), nil
	case SimpleStringByte:
		s, err := readNext(reader)
		if err != nil {
			fmt.Printf("failed to read simple string: %v\n", err)
			return nil, err
		}
		return SimpleString(strings.ToLower(string(s))), nil
	default:
		fmt.Printf("unexpected data type: %v\n", firstByte)
		return nil, errors.New("unexpected data type")
	}
}

func readNext(reader *bufio.Reader) ([]byte, error) {
	var bytes bytes.Buffer
	for {
		next, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		var length = len(next)
		if length == 1 {
			bytes.Write(next)
			continue
		}
		if next[length-2] == '\r' {
			bytes.Write(next[0 : length-2])
			return bytes.Bytes(), nil
		}
	}
}

func readExact(reader *bufio.Reader, count int) ([]byte, error) {
	buf := make([]byte, count)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	for _, char := range [2]byte {'\r', '\n'} {
		next, err := reader.ReadByte()
		if err != nil || next != char {
			reader.UnreadByte()
			break
		}
	}

	return buf, nil
}

func readInt(reader *bufio.Reader) (int64, error) {
	lengthBytes, err := readNext(reader)
	if err != nil {
		fmt.Printf("unable to read length: %v", err)
		return 0, err
	}
	length, err := strconv.ParseInt(string(lengthBytes), 10, 64)
	if err != nil {
		fmt.Printf("failed to convert length to int: %v", err)
		return 0, err
	}
	return length, err
}

func (a Array) Bytes() []byte {
	var bytes bytes.Buffer
	bytes.WriteByte(ArrayByte)
	bytes.Write([]byte(strconv.Itoa(len(a.Content))))
	writeTerminator(&bytes)
	for _, element := range a.Content {
		bytes.Write(element.Bytes())
	}
	return bytes.Bytes()
}

func (s BulkString) Bytes() []byte {
	var bytes bytes.Buffer
	bytes.WriteByte(BulkStringByte)
	bytes.Write([]byte(strconv.Itoa(len(s))))
	writeTerminator(&bytes)
	bytes.Write([]byte(s))
	writeTerminator(&bytes)
	return bytes.Bytes()
}

func (s NullBulkString) Bytes() []byte {
	var bytes bytes.Buffer
	bytes.WriteByte(BulkStringByte)
	bytes.Write([]byte(strconv.Itoa(-1)))
	writeTerminator(&bytes)
	return bytes.Bytes()
}

func (s SimpleString) Bytes() []byte {
	var bytes bytes.Buffer
	bytes.WriteByte(SimpleStringByte)
	bytes.Write([]byte(s))
	writeTerminator(&bytes)
	return bytes.Bytes()
}

func (i Integer) Bytes() []byte {
	var bytes bytes.Buffer
	bytes.WriteByte(IntegerByte)
	if i < 0 {
		bytes.WriteByte('-')
	}
	bytes.Write([]byte(strconv.Itoa(int(i))))
	writeTerminator(&bytes)
	return bytes.Bytes()
}

func writeTerminator(w io.ByteWriter) {
	w.WriteByte('\r')
	w.WriteByte('\n')
}

func String(r RespDataType) string {
	switch t := r.(type) {
	case BulkString:
		return string(t)
	case SimpleString:
		return string(t)
	default:
		return ""
	}
}
