package h2go

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/text/encoding/unicode"
)

// Value types
const (
	Null             int32 = 0
	Boolean          int32 = 1
	Byte             int32 = 2
	Short            int32 = 3
	Int              int32 = 4
	Long             int32 = 5
	Decimal          int32 = 6
	Double           int32 = 7
	Float            int32 = 8
	Time             int32 = 9
	Date             int32 = 10
	Timestamp        int32 = 11
	Bytes            int32 = 12
	String           int32 = 13
	StringIgnoreCase int32 = 14
	Blob             int32 = 15
	Clob             int32 = 16
	Array            int32 = 17
	ResultSet        int32 = 18
	JavaObject       int32 = 19
	UUID             int32 = 20
	StringFixed      int32 = 21
	Geometry         int32 = 22
	TimestampTZ      int32 = 24
	Enum             int32 = 25
	Interval         int32 = 26
	Row              int32 = 27
	JSON             int32 = 28
	TimeTZ           int32 = 29
)

type transfer struct {
	conn net.Conn
	buff *bufio.ReadWriter
}

func newTransfer(conn net.Conn) transfer {

	buffReader := bufio.NewReader(conn)
	buffWriter := bufio.NewWriter(conn)
	buff := bufio.NewReadWriter(buffReader, buffWriter)
	return transfer{conn: conn, buff: buff}
}

func (t *transfer) readInt32() (int32, error) {
	var ret int32
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read int value from socket")
	}
	return ret, nil

}

func (t *transfer) writeInt32(v int32) error {
	return binary.Write(t.buff, binary.BigEndian, v)
}

func (t *transfer) writeInt64(v int64) error {
	return binary.Write(t.buff, binary.BigEndian, v)
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
	buf := make([]byte, n*2)
	/*
		var cur int32
		for {
			n2, err := t.buff.Read(buf[cur:n])
			if err != nil {
				return "", err
			}
			cur += int32(n2)
			if cur == n {
				break
			}
		}
	*/
	n2, err := t.buff.Read(buf)
	if err != nil {
		return "", err
	}
	if n2 != len(buf) {
		return "", errors.Errorf("Can't read all data needed")
	}
	dec := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder()
	buf, err = dec.Bytes(buf)
	if err != nil {
		return "", errors.Wrapf(err, "can't convert from UTF-16 a UTF-8 string")
	}
	return string(buf), nil

}

func (t *transfer) writeString(s string) error {
	// log.Printf("write: %s", s)
	var err error
	data := []byte(s)
	// var pos int32
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
	/*
		n = int32(len(data))
		for {
			n2, err := t.buff.Write(data[pos:n])
			if err != nil {
				return errors.Wrapf(err, "can't write string to socket")
			}
			pos += int32(n2)
			if pos == n {
				break
			}
		}
	*/
	n2, err := t.buff.Write(data)
	if err != nil {
		return errors.Wrapf(err, "can't write string to socket")
	}
	if n2 != len(data) {
		return errors.Errorf("Data send not equal to wished")
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
	return t.readBytesDef(int(n))

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
	n, err := t.buff.Write(data)
	if err != nil {
		return errors.Wrapf(err, "can't write bytes to socket")
	}
	if int32(n) != s {
		return errors.Wrapf(err, "can't write all bytes to socket => %d != %d", n, s)
	}
	return nil
}

func (t *transfer) readBool() (bool, error) {
	v, err := t.readByte()
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

func (t *transfer) readByte() (byte, error) {
	v, err := t.buff.ReadByte()
	return v, err
}

func (t *transfer) readLong() (int64, error) {
	var ret int64
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read long value from socket")
	}
	return ret, nil
}

func (t *transfer) flush() error {
	return t.buff.Flush()
}

func (t *transfer) readValue() (interface{}, error) {
	var err error
	kind, err := t.readInt32()
	if err != nil {
		return nil, errors.Wrapf(err, "can't read type of value")
	}
	switch kind {
	case Null:
		// TODO: review
		return nil, nil
	case Bytes:
		return t.readBytes()
	case UUID:
		return nil, errors.Errorf("UUID not implemented")
	case JavaObject:
		return nil, errors.Errorf("Java Object not implemented")
	case Boolean:
		return t.readBool()
	case Byte:
		return t.readByte()
	case Date:
		return nil, errors.Errorf("Date not implemented")
	case Time:
		return nil, errors.Errorf("Time not implemented")
	case TimeTZ:
		return nil, errors.Errorf("Time TZ not implemented")
	case Timestamp:
		return nil, errors.Errorf("Timestamp not implemented")
	case TimestampTZ:
		return nil, errors.Errorf("Timestamp TZ not implemented")
	case Decimal:
		return nil, errors.Errorf("Decimal not implemented")
	case Double:
		return nil, errors.Errorf("Double not implemented")
	case Float:
		return nil, errors.Errorf("Float not implemented")
	case Enum:
		return nil, errors.Errorf("Enum not implemented")
	case Int:
		return t.readInt32()
	case Long:
		return t.readLong()
	case Short:
		return nil, errors.Errorf("Short not implemented")
	case String:
		return t.readString()
	case StringIgnoreCase:
		return t.readString()
	case StringFixed:
		return t.readString()
	case Blob:
		return nil, errors.Errorf("Blob not implemented")
	case Clob:
		return nil, errors.Errorf("Clob not implemented")
	case Array:
		return nil, errors.Errorf("Array not implemented")
	case Row:
		return nil, errors.Errorf("Row not implemented")
	case ResultSet:
		return nil, errors.Errorf("Result Set not implemented")
	case Geometry:
		return nil, errors.Errorf("Geometry not implemented")
	case JSON:
		return nil, errors.Errorf("JSON not implemented")
	default:
		return nil, errors.Errorf("Unknown type: %d", kind)
	}

}

func (t *transfer) writeValue(v interface{}) error {
	switch kind := v.(type) {
	case nil:
		t.writeInt32(Null)
	case int:
		s := unsafe.Sizeof(v)
		if s == 4 {
			t.writeInt32(Int)
			t.writeInt32(int32(v.(int)))
		} else {
			// 8 bytes
			t.writeInt32(Long)
			t.writeInt64(int64(v.(int)))
		}
	case int32:
		t.writeInt32(Int)
		t.writeInt32(int32(v.(int32)))
	case int64:
		t.writeInt32(Long)
		t.writeInt64(int64(v.(int64)))
	case string:
		t.writeInt32(String)
		t.writeString(v.(string))
	// case time.Time:
	default:
		return fmt.Errorf("Can't convert type %v to H2 Type", kind)
	}
	return nil
}

func (t *transfer) readBytesDef(n int) ([]byte, error) {

	buf := make([]byte, n)
	n2, err := t.buff.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != n2 {
		return nil, errors.Errorf("Read byte size differs: %d != %d", n, n2)
	}
	return buf, nil

}
