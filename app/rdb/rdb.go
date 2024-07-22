package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"strconv"
)

const (
	firstByteSuffixMask = 0b11000000
	firstBytePrefixMask = 0b00111111
	singleByteIntMask   = 0
	twoByteIntMask      = 0b01000000
	fourByteIntMask     = 0b10000000
	specialFormatMask   = 0b11000000
)

func Empty() ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	_, err := buffer.Write([]byte("REDIS0011"))
	if err != nil {
		return nil, err
	}
	err = writeAUX(buffer, map[string]interface{}{
		"redis-ver":  "7.2.0",
		"redis-bits": 64,
		"ctime":      1829289061,
		"used-mem":   2965639168,
		"aof-base":   0,
	})
	if err != nil {
		return nil, err
	}
	err = buffer.WriteByte(0xFF)
	if err != nil {
		return nil, err
	}
	bytes, err := appendChecksum(buffer)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func writeAUX(buffer *bytes.Buffer, fields map[string]interface{}) error {
	for key, value := range fields {
		err := buffer.WriteByte(0xFA)
		if err != nil {
			return err
		}
		err = writeString(key, buffer)
		if err != nil {
			return err
		}
		switch v := value.(type) {
		case int:
			_, err = buffer.Write(encodeUInt32(uint32(v)))
		case string:
			err = writeString(v, buffer)
		default:
			err = fmt.Errorf("unexpected aux value type. should be string or int")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func writeString(s string, buffer *bytes.Buffer) error {
	_, err := buffer.Write(encodeLen(int32(len(s))))
	if err != nil {
		return err
	}
	_, err = buffer.Write([]byte(s))
	return err
}

func encodeLen(len int32) []byte {
	if len <= 63 {
		return []byte{byte(len)}
	} else if len <= 16383 {
		return []byte{twoByteIntMask | byte(len>>8), byte(len & 0b11111111)}
	} else {
		var bytes [5]byte
		bytes[0] = fourByteIntMask
		binary.BigEndian.PutUint32(bytes[1:], uint32(len))
		return bytes[:]
	}
}

func encodeUInt32(num uint32) []byte {
	if num <= 255 {
		return []byte{specialFormatMask, byte(num)}
	} else if num <= 65535 {
		var bytes [3]byte
		bytes[0] = specialFormatMask | 1
		binary.BigEndian.PutUint16(bytes[1:], uint16(num))
		return bytes[:]
	} else {
		var bytes [5]byte
		bytes[0] = specialFormatMask | 2
		binary.BigEndian.PutUint32(bytes[1:], num)
		return bytes[:]
	}
}

func appendChecksum(buf *bytes.Buffer) ([]byte, error) {
	checksum := crc64.Checksum(buf.Bytes(), crc64.MakeTable(crc64.ECMA))
	bytes := binary.BigEndian.AppendUint64(buf.Bytes(), checksum)
	return bytes, nil
}

func DecodeLen(reader *bufio.Reader) (int32, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	switch firstByte & firstByteSuffixMask {
	case singleByteIntMask:
		return 0b111111 & int32(firstByte), nil
	case twoByteIntMask:
		secondByte, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return int32(secondByte) + ((int32(firstByte) & firstBytePrefixMask) << 8), nil
	case fourByteIntMask:
		var lengthBytes [4]byte
		_, err := io.ReadFull(reader, lengthBytes[:])
		if err != nil {
			return 0, nil
		}
		return int32(binary.BigEndian.Uint32(lengthBytes[:])), nil
	case specialFormatMask:
		return 0, fmt.Errorf("special format not supported yet")
	default:
		return 0, fmt.Errorf("unknown first byte suffix: %v", strconv.FormatInt(int64(firstByte), 2))
	}
}
