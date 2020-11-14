/*
Copyright 2020 JM Robles (@jmrobles)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package h2go

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
	TimeTZQuery      int32 = 29
	TimeTZ           int32 = 41
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
		return -1, errors.Wrapf(err, "can't read int32 value from socket")
	}
	return ret, nil
}
func (t *transfer) readInt16() (int16, error) {
	n, err := t.readInt32()
	if err != nil {
		return int16(-1), err
	}
	return int16(n), err
}
func (t *transfer) readInt64() (int64, error) {
	var ret int64
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read int64 value from socket")
	}
	return ret, nil
}

func (t *transfer) readFloat32() (float32, error) {
	var ret float32
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read float32 value from socket")
	}
	return ret, nil
}

func (t *transfer) readFloat64() (float64, error) {
	var ret float64
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read float64 value from socket")
	}
	return ret, nil
}

func (t *transfer) writeInt32(v int32) error {
	return binary.Write(t.buff, binary.BigEndian, v)
}

func (t *transfer) writeInt64(v int64) error {
	return binary.Write(t.buff, binary.BigEndian, v)
}
func (t *transfer) writeFloat64(v float64) error {
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
	var err error
	data := []byte(s)
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
func (t *transfer) writeBool(b bool) error {
	var v byte = 0
	if b {
		v = 1
	}
	return t.writeByte(v)
}

func (t *transfer) writeByte(b byte) error {
	return t.buff.WriteByte(b)
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
func (t *transfer) readDate() (time.Time, error) {
	n, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2date(n)
	return date, nil
}

func (t *transfer) readTimestamp() (time.Time, error) {
	nDate, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2ts(nDate, nNsecs)
	return date, nil
}

func (t *transfer) readTimestampTZ() (time.Time, error) {
	nDate, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nDiffTZ, err := t.readInt32()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2tsz(nDate, nNsecs, nDiffTZ)
	return date, nil
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
	L(log.DebugLevel, "Value type: %d", kind)
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
		return t.readDate()
	case Time:
		return t.readTime()
	case TimeTZQuery, TimeTZ:
		return t.readTimeTZ()
	case Timestamp:
		return t.readTimestamp()
	case TimestampTZ:
		return t.readTimestampTZ()
	case Decimal:
		return nil, errors.Errorf("Decimal not implemented")
	case Double:
		return t.readFloat64()
	case Float:
		return t.readFloat32()
	case Enum:
		return nil, errors.Errorf("Enum not implemented")
	case Int:
		return t.readInt32()
	case Long:
		return t.readLong()
	case Short:
		return t.readInt16()
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
		L(log.ErrorLevel, "Unknown type: %d", kind)
		return nil, errors.Errorf("Unknown type: %d", kind)
	}

}

func (t *transfer) writeValue(v interface{}) error {
	switch kind := v.(type) {
	case nil:
		t.writeInt32(Null)
	case bool:
		t.writeInt32(Boolean)
		t.writeBool(v.(bool))
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
	case int16:
		t.writeInt32(Short)
		t.writeInt32(v.(int32))
	case int32:
		t.writeInt32(Int)
		t.writeInt32(int32(v.(int32)))
	case int64:
		t.writeInt32(Long)
		t.writeInt64(int64(v.(int64)))
	case float64:
		t.writeInt32(Double)
		t.writeFloat64(v.(float64))
	case string:
		t.writeInt32(String)
		t.writeString(v.(string))
	case byte:
		t.writeInt32(Byte)
		t.writeByte(v.(byte))
	case []byte:
		t.writeInt32(Bytes)
		t.writeBytes(v.([]byte))
	// case time.Time:
	default:
		return fmt.Errorf("Can't convert type %T to H2 Type", kind)
	}
	return nil
}
func (t *transfer) writeDatetimeValue(dt time.Time, mdp h2parameter) error {
	L(log.DebugLevel, "Date/time type: %d", mdp.kind)
	var err error
	switch mdp.kind {
	case Date:
		t.writeInt32(Date)
		bin := date2bin(&dt)
		err = t.writeInt64(bin)
		if err != nil {
			return err
		}
	case Timestamp:
		t.writeInt32(Timestamp)
		dateBin, nsecBin := ts2bin(&dt)
		err = t.writeInt64(dateBin)
		if err != nil {
			return err
		}
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
	case TimestampTZ:
		t.writeInt32(TimestampTZ)
		dateBin, nsecBin, offsetTZBin := tsz2bin(&dt)
		err = t.writeInt64(dateBin)
		if err != nil {
			return err
		}
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
		err = t.writeInt32(offsetTZBin)
		if err != nil {
			return err
		}
	case Time:
		t.writeInt32(Time)
		nsecBin := time2bin(&dt)
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
	case TimeTZ:
		t.writeInt32(TimeTZQuery)
		nsecBin, offsetTZBin := timetz2bin(&dt)
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
		err = t.writeInt32(offsetTZBin)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Datatype unsupported: %d", mdp.kind)
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
func (t *transfer) close() error {
	// TODO: check close
	return nil
}

// Helpers

func date2bin(dt *time.Time) int64 {
	return int64((dt.Year() << 9) + (int(dt.Month()) << 5) + dt.Day())
}

func bin2date(n int64) time.Time {
	day := int(n & 0x1f)
	month := time.Month((n >> 5) & 0xf)
	year := int(n >> 9)
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func ts2bin(dt *time.Time) (int64, int64) {
	var nsecBin int64
	dateBin := date2bin(dt)
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	return dateBin, nsecBin
}

func bin2ts(dateBin int64, nsecBin int64) time.Time {
	// TODO: optimization
	day := int(dateBin & 0x1f)
	month := time.Month((dateBin >> 5) & 0xf)
	year := int(dateBin >> 9)
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	return time.Date(year, month, day, hour, minute, sec, nsecs, time.UTC)
}

func bin2tsz(dateBin int64, nsecBin int64, secsTZ int32) time.Time {
	// TODO: optimization
	day := int(dateBin & 0x1f)
	month := time.Month((dateBin >> 5) & 0xf)
	year := int(dateBin >> 9)
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	tz := time.FixedZone(fmt.Sprintf("tz_%d", secsTZ), int(secsTZ))
	return time.Date(year, month, day, hour, minute, sec, nsecs, tz)
}

func tsz2bin(dt *time.Time) (int64, int64, int32) {
	var nsecBin int64
	dateBin := date2bin(dt)
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	_, offsetTZ := dt.Zone()
	return dateBin, nsecBin, int32(offsetTZ)
}

func time2bin(dt *time.Time) int64 {
	var nsecBin int64
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	return nsecBin
}

func bin2time(nsecBin int64) time.Time {
	// TODO: optimization
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	return time.Date(0, 1, 1, hour, minute, sec, nsecs, time.UTC)
}

func (t *transfer) readTime() (time.Time, error) {
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2time(nNsecs)
	return date, nil
}

func (t *transfer) readTimeTZ() (time.Time, error) {
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nDiffTZ, err := t.readInt32()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2timetz(nNsecs, nDiffTZ)
	return date, nil
}

func bin2timetz(nsecBin int64, secsTZ int32) time.Time {
	// TODO: optimization
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	tz := time.FixedZone(fmt.Sprintf("tz_%d", secsTZ), int(secsTZ))
	return time.Date(0, 1, 1, hour, minute, sec, nsecs, tz)
}

func timetz2bin(dt *time.Time) (int64, int32) {
	var nsecBin int64
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	_, offsetTZ := dt.Zone()
	return nsecBin, int32(offsetTZ)
}
