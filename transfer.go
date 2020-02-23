package h2go

import (
	"encoding/binary"
	"log"
	"net"

	"github.com/pkg/errors"
	"golang.org/x/text/encoding/unicode"
)

type transfer struct {
	conn net.Conn
}

func newTransfer(conn net.Conn) transfer {
	return transfer{conn: conn}
}

func (t *transfer) readInt32() (int32, error) {
	var ret int32
	err := binary.Read(t.conn, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read int value from socket")
	}
	return ret, nil

}

func (t *transfer) writeInt32(v int32) error {
	return binary.Write(t.conn, binary.BigEndian, v)
}

func (t *transfer) readString() (string, error) {
	var err error
	n, err := t.readInt32()
	if err != nil {
		return "", errors.Wrapf(err, "can't read string length from socket")
	}
	if n == -1 || n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	var cur int32
	for {
		n2, err := t.conn.Read(buf[cur:n])
		if err != nil {
			return "", err
		}
		cur += int32(n2)
		if cur == n {
			break
		}
	}
	return string(buf), nil

}

func (t *transfer) writeString(s string) error {
	log.Printf("write: %s", s)
	var err error
	data := []byte(s)
	var pos int32
	n := int32(len(data))
	if n == 0 {
		n = -1
	}
	err = t.writeInt32(n)
	if err != nil {
		return errors.Wrapf(err, "can't write string length to socket")
	}
	if n == -1 {
		return nil
	}
	enc := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewEncoder()
	data, err = enc.Bytes(data)
	if err != nil {
		return errors.Wrapf(err, "can't convert to UTF-16")
	}
	n = int32(len(data))
	for {
		n2, err := t.conn.Write(data[pos:n])
		if err != nil {
			return errors.Wrapf(err, "can't write string to socket")
		}
		pos += int32(n2)
		if pos == n {
			break
		}
	}
	return nil
}

func (t *transfer) readBytes() ([]byte, error) {
	n, err := t.readInt32()
	if err != nil {
		return nil, errors.Wrapf(err, "can't read bytes length from socket")
	}
	if n == -1 {
		return nil, nil
	}
	buf := make([]byte, n)
	n2, err := t.conn.Read(buf)
	if n != int32(n2) {
		return nil, errors.Errorf("Read byte size differs: %d != %d", n, n2)
	}
	return buf, nil

}

func (t *transfer) writeBytes(data []byte) error {
	var err error
	s := int32(len(data))
	if data == nil || s == 0 {
		s = -1
	}
	err = t.writeInt32(s)
	if err != nil {
		return errors.Wrapf(err, "can't write bytes length to socket")
	}
	if s == -1 {
		return nil
	}
	n, err := t.conn.Write(data)
	if err != nil {
		return errors.Wrapf(err, "can't write bytes to socket")
	}
	if int32(n) != s {
		return errors.Wrapf(err, "can't write all bytes to socket => %d != %d", n, s)
	}
	return nil
}
