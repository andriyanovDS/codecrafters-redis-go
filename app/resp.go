package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	IntegerByte    = ':'
	StringByte     = '+'
	ErrorByte      = '-'
	ArrayByte      = '*'
	BulkStringByte = '$'
)

type RespDataType interface {
	Bytes() []byte
}

type Array struct {
	Content []RespDataType
}

type String struct {
	Content string
}

func Parse(reader *bufio.Reader) (RespDataType, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		fmt.Printf("failed to read byte: %v", err)
		return nil, err
	}
	fmt.Printf("first byte: %v\n", firstByte)
	switch firstByte {
	case BulkStringByte:
		fmt.Println("Bulk string received")
		length, err := readLendth(reader)
		fmt.Printf("Bulk string length %v\n", length)
		if err != nil {
			return nil, err
		}
		bytes, err := readExact(reader, int(length))
		if err != nil {
			fmt.Printf("failed to read bulk string: %v\n", err)
			return nil, err
		}
		return String{Content: string(bytes)}, nil
	case ArrayByte:
		fmt.Println("Array received")
		length, err := readLendth(reader)
		fmt.Printf("Array length %v\n", length)
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
	buf := make([]byte, count+2)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	if buf[count] == '\r' && buf[count+1] == '\n' {
		return buf[0:count], nil
	} else {
		return nil, errors.New("invalid RESP termination")
	}
}

func readLendth(reader *bufio.Reader) (int64, error) {
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
	bytes.Write([]byte(string(len(a.Content))))
	writeTerminator(&bytes)
	for _, element := range a.Content {
		bytes.Write(element.Bytes())
		writeTerminator(&bytes)
	}
	writeTerminator(&bytes)
	return bytes.Bytes()
}

func (s String) Bytes() []byte {
	var bytes bytes.Buffer
	bytes.WriteByte(BulkStringByte)
	bytes.Write([]byte(strconv.Itoa(len(s.Content))))
	writeTerminator(&bytes)
	bytes.Write([]byte(s.Content))
	writeTerminator(&bytes)
	return bytes.Bytes()
}

func writeTerminator(w io.ByteWriter) {
	w.WriteByte('\r')
	w.WriteByte('\n')
}
