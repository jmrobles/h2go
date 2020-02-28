package h2go

import (
	"database/sql/driver"
)

type session struct {
	seqID int32
}

func newSession() session {
	return session{}
}

func (s *session) prepare(t *transfer, sql string, args []driver.Value) error {
	var err error
	// 1. Write ID
	id := s.getNextID()
	err = t.writeInt32(id)
	if err != nil {
		return err
	}
	// 2. Write SQL text
	err = t.writeString(sql)
	if err != nil {
		return err
	}
	// 3. Write Old Mod ID
	// TODO: implement it
	err = t.writeInt32(0)
	if err != nil {
		return err
	}
	// 4. Flush data and wait server info
	err = t.flush()
	if err != nil {
		return err
	}
	// 5. Read old state
	state, err := t.readInt32()
	if err != nil {
		return err
	}
	// 6. Read Is Query
	isQuery, err := t.readBool()
	if err != nil {
		return err
	}
	// 7. Read Is Read-only
	isRO, err := t.readBool()
	if err != nil {
		return err
	}

	return nil
}

func (s *session) getNextID() int32 {
	s.seqID++
	return s.seqID
}
