package h2go

import (
	"database/sql/driver"
	"log"
)

const (
	sessionPrepare               = 0
	sessionClose                 = 1
	sessionCommandExecuteQuery   = 2
	sessionCommandExecuteUpdate  = 3
	sessionCommandClose          = 4
	sessionResultFetchRows       = 5
	sessionResultReset           = 6
	sessionResultClose           = 7
	sessionCommandCommit         = 8
	sessionChangeID              = 9
	sessionCommandgetMetaData    = 10
	sessionPrepareReadParams     = 11
	sessionSetID                 = 12
	sessionCancelStatement       = 13
	sessionCheckKey              = 14
	sessionSetAutocommit         = 15
	sessionHasPendingTransaction = 16
	sessionLobRead               = 17
	sessionPrepareReadParams2    = 18

	sessionStatusError          = 0
	sessionStatusOk             = 1
	sessionStatusClosed         = 2
	sessionStatusOkStateChanged = 3
)

type session struct {
	seqID int32
}

func newSession() session {
	return session{}
}

func (s *session) prepare(t *transfer, sql string, args []driver.Value) (driver.Stmt, error) {
	var err error
	stmt := h2stmt{}
	// 0. Write SESSION_PREPARE
	err = t.writeInt32(sessionPrepare)
	// 1. Write ID
	stmt.id = s.getNextID()
	err = t.writeInt32(stmt.id)
	if err != nil {
		return stmt, err
	}
	// 2. Write SQL text
	err = t.writeString(sql)
	if err != nil {
		return stmt, err
	}
	// 3. Write Old Mod ID
	// TODO: implement it
	err = t.writeInt32(0)
	if err != nil {
		return stmt, err
	}
	// 4. Flush data and wait server info
	err = t.flush()
	if err != nil {
		return stmt, err
	}
	// 5. Read old state
	state, err := t.readInt32()
	if err != nil {
		return stmt, err
	}
	// 6. Read Is Query
	isQuery, err := t.readBool()
	if err != nil {
		return stmt, err
	}
	// 7. Read Is Read-only
	isRO, err := t.readBool()
	if err != nil {
		return stmt, err
	}
	// 8. Read params size
	numParams, err := t.readInt32()
	if err != nil {
		return stmt, err
	}
	log.Printf("STATE: %d, IsQuery: %v, Is Read-Only: %v, Num Params: %d", state, isQuery, isRO, numParams)

	return stmt, nil
}

func (s *session) getNextID() int32 {
	s.seqID++
	return s.seqID
}
