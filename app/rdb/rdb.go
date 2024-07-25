package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

const (
	firstByteSuffixMask = 0b11000000
	firstBytePrefixMask = 0b00111111
	singleByteIntMask   = 0
	twoByteIntMask      = 0b01000000
	fourByteIntMask     = 0b10000000
	specialFormatMask   = 0b11000000
	int8Mask            = 0b11000000
	int16Mask           = 0b11000001
	int32Mask           = 0b11000010
)

const (
	unixTimestampSecByte = 0xfd
	unixTimestampMsByte  = 0xfc
	auxSectionByte       = 0xfa
	dbSectionByte        = 0xfe
	resizedbByte         = 0xfb
	eofByte              = 0xff
)

const (
	stringValueTypeByte = 0x0
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

type ReadStrategy interface {
	AddDbEntry(DbEntry)
	AddAux(key string, value resp.RespDataType)
}

type DbEntry struct {
	Key      resp.BulkString
	Value    resp.RespDataType
	ExpireAt time.Time
}

func Read(reader *bufio.Reader, strategy ReadStrategy) error {
	var header [9]byte
	_, err := io.ReadFull(reader, header[:])
	if err != nil {
		return err
	}
	fmt.Printf("reading RDB file version: %s\n", string(header[5:]))
	err = decodeMetadata(reader, strategy)
	if err != nil {
		return err
	}
	for {
		err = decodeDBSection(reader, strategy)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func decodeMetadata(reader *bufio.Reader, strategy ReadStrategy) error {
	for {
		firstByte, err := reader.ReadByte()
		if err != nil {
			return err
		}
		if firstByte != auxSectionByte {
			reader.UnreadByte()
			fmt.Printf("Metadata section ended\n")
			return nil
		}
		fmt.Printf("Reading metadata\n")
		key, err := decodeString(reader)
		if err != nil {
			return err
		}
		value, err := decodeString(reader)
		if err != nil {
			return err
		}
		fmt.Printf("AUX key: %v, value: %v\n", key, value)
		strategy.AddAux(string(key.(resp.BulkString)), value)
	}
}

func decodeDBSection(reader *bufio.Reader, strategy ReadStrategy) error {
	sectionByte, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if sectionByte != dbSectionByte {
		return fmt.Errorf("expect db section byte %x, got: %x", dbSectionByte, sectionByte)
	}
	index, err := reader.ReadByte()
	if err != nil {
		return err
	}
	fmt.Printf("Decodig db at index %d\n", index)
	field, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if field != resizedbByte {
		return fmt.Errorf("resizedb field expected")
	}
	keyValueTableSize, err := decodeLenEncoded(reader)
	if err != nil {
		return err
	}
	expireTableSize, err := decodeLenEncoded(reader)
	if err != nil {
		return err
	}
	fmt.Printf("Key-value table size: %v, expire table size %v\n", keyValueTableSize, expireTableSize)

	var expireAt time.Time
	for {
		firstByte, err := reader.ReadByte()
		if err != nil {
			return err
		}
		switch firstByte {
		case unixTimestampSecByte:
			var bytes [4]byte
			_, err := io.ReadFull(reader, bytes[:])
			if err != nil {
				return err
			}
			seconds := binary.LittleEndian.Uint32(bytes[:])
			expireAt = time.Unix(int64(seconds), 0)
		case unixTimestampMsByte:
			var bytes [8]byte
			_, err := io.ReadFull(reader, bytes[:])
			if err != nil {
				return err
			}
			ms := binary.LittleEndian.Uint64(bytes[:])
			expireAt = time.UnixMilli(int64(ms))
		case stringValueTypeByte:
			key, err := decodeString(reader)
			if err != nil {
				return err
			}
			value, err := decodeString(reader)
			if err != nil {
				return err
			}
			fmt.Printf("DB entity key: %v, value: %v, expires: %v\n", key, value, expireAt)
			strategy.AddDbEntry(DbEntry{
				Key:      key.(resp.BulkString),
				Value:    value,
				ExpireAt: expireAt,
			})
		case dbSectionByte:
			reader.UnreadByte()
			return nil
		case eofByte:
			return io.EOF
		default:
			return fmt.Errorf("unsupported value type: %d", firstByte)
		}
	}
}

func decodeString(reader *bufio.Reader) (resp.RespDataType, error) {
	length, err := decodeLenEncoded(reader)
	_, ok := err.(specialFormatEncoding)
	if ok {
		return decodeSpecialFormat(reader)
	} else if err != nil {
		return nil, err
	} else {
		bytes := make([]byte, length.(resp.Integer))
		_, err = io.ReadFull(reader, bytes)
		if err != nil {
			return nil, err
		}
		return resp.BulkString(string(bytes)), nil
	}
}

func decodeLenEncoded(reader *bufio.Reader) (resp.RespDataType, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch firstByte & firstByteSuffixMask {
	case singleByteIntMask:
		return resp.Integer(0xff & int32(firstByte)), nil
	case twoByteIntMask:
		secondByte, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		i := int32(secondByte) + ((int32(firstByte) & firstBytePrefixMask) << 8)
		return resp.Integer(i), nil
	case fourByteIntMask:
		var lengthBytes [4]byte
		_, err := io.ReadFull(reader, lengthBytes[:])
		if err != nil {
			return nil, nil
		}
		return resp.Integer(int32(binary.BigEndian.Uint32(lengthBytes[:]))), nil
	case specialFormatMask:
		err := reader.UnreadByte()
		if err != nil {
			return nil, err
		}
		return nil, specialFormatEncoding{firstByte: firstByte}
	default:
		return nil, fmt.Errorf("unknown length encoding prefix: %v", strconv.FormatInt(int64(firstByte), 2))
	}
}

func decodeSpecialFormat(reader *bufio.Reader) (resp.RespDataType, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch firstByte {
	case int8Mask:
		next, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		return resp.Integer(int32(next)), nil
	case int16Mask:
		var bytes [2]byte
		_, err := io.ReadFull(reader, bytes[:])
		if err != nil {
			return nil, err
		}
		return resp.Integer(int32(binary.BigEndian.Uint16(bytes[:]))), nil
	case int32Mask:
		var bytes [4]byte
		_, err := io.ReadFull(reader, bytes[:])
		if err != nil {
			return nil, err
		}
		return resp.Integer(int32(binary.BigEndian.Uint32(bytes[:]))), nil
	default:
		return nil, fmt.Errorf("unsupported string encoding: %d", firstByte)
	}
}

type specialFormatEncoding struct {
	firstByte byte
}

func (s specialFormatEncoding) Error() string {
	return fmt.Sprintf("special format encoding %b", s.firstByte)
}
