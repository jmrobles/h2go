package h2go

import (
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

func (s *session) prepare(t *transfer, sql string, args []driver.NamedValue) (driver.Stmt, error) {
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
	err = s.checkSQLError(state, t)
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

func (s *session) executeQuery(stmt *h2stmt, t *transfer) ([]string, int32, error) {
	var err error
	// 0. Write COMMAND EXECUTE QUERY
	log.Printf("Execute query")
	err = t.writeInt32(sessionCommandExecuteQuery)
	if err != nil {
		return nil, -1, err
	}
	// 1. Write ID of query
	//st := (*stmt).(h2stmt)
	err = t.writeInt32(stmt.id)
	if err != nil {
		return nil, -1, err
	}
	// 2. Write Object ID
	stmt.oID = s.getNextID()
	err = t.writeInt32(stmt.oID)
	if err != nil {
		return nil, -1, err
	}
	// 3. Write Max rows
	err = t.writeInt32(200)
	if err != nil {
		return nil, -1, err
	}
	// 4. Write Fetch max size
	err = t.writeInt32(64)
	if err != nil {
		return nil, -1, err
	}
	// 4. Write Fetch max size
	err = t.writeInt32(0)
	if err != nil {
		return nil, -1, err
	}

	// 5. Flush data
	err = t.flush()
	if err != nil {
		return nil, -1, err
	}
	// Read query status
	status, err := t.readInt32()
	if err != nil {
		return nil, -1, err
	}
	/*
		err = s.checkSQLError(status, t)
		if err != nil {
			return nil, -1, err
		}
	*/
	colCnt, err := t.readInt32()
	if err != nil {
		return nil, -1, err
	}
	rowCnt, err := t.readInt32()
	if err != nil {
		return nil, -1, err
	}
	L(log.DebugLevel, "Status: %d - Num cols: %d - Num rows: %d", status, colCnt, rowCnt)
	cols, err := s.readColumns(t, colCnt)
	if err != nil {
		return nil, -1, err
	}

	return cols, rowCnt, nil
}
func (s *session) readColumns(t *transfer, colCnt int32) ([]string, error) {
	// Alias
	cols := []string{}
	for i := 0; i < int(colCnt); i++ {
		alias, err := t.readString()
		if err != nil {
			return nil, err
		}
		// Schema
		// Ignored
		_, err = t.readString()
		if err != nil {
			return nil, err
		}
		// TableName
		// Ignored
		_, err = t.readString()
		if err != nil {
			return nil, err
		}
		// Column name
		colName, err := t.readString()
		if err != nil {
			return nil, err
		}
		// Skip other info
		// - Value type (int)
		_, err = t.readInt32()
		if err != nil {
			return nil, err
		}
		// - Precision (long)
		_, err = t.readLong()
		if err != nil {
			return nil, err
		}
		// - Scale (int)
		_, err = t.readInt32()
		if err != nil {
			return nil, err
		}
		// - Display Size (int)
		_, err = t.readInt32()
		if err != nil {
			return nil, err
		}
		// - Autoincrement (bool)
		_, err = t.readBool()
		if err != nil {
			return nil, err
		}
		// - Nullable (int)
		_, err = t.readInt32()
		if err != nil {
			return nil, err
		}
		// Set columns name
		if alias != "" {
			cols = append(cols, alias)
		} else {
			cols = append(cols, colName)
		}
	}
	return cols, nil

}
func (s *session) getNextID() int32 {
	s.seqID++
	return s.seqID
}

type h2error struct {
	strError  string
	msg       string
	sql       string
	codeError int32
	trace     string
	error
}

func (s *session) checkSQLError(state int32, t *transfer) error {
	if state == 1 {
		return nil
	}
	// SQL Error
	sqlError, err := t.readString()
	if err != nil {
		return errors.Wrapf(err, "SQL Error: unknown")
	}
	sqlMsg, err := t.readString()
	if err != nil {
		return errors.Wrapf(err, "SQL Error: unknown")
	}
	sqlSQL, err := t.readString()
	if err != nil {
		return errors.Wrapf(err, "SQL Error: unknown")
	}
	errCode, err := t.readInt32()
	if err != nil {
		return errors.Wrapf(err, "SQL Error: unknown")
	}
	sqlTrace, err := t.readString()
	if err != nil {
		return errors.Wrapf(err, "SQL Error: unknown")
	}

	return newError(sqlError, sqlMsg, sqlSQL, errCode, sqlTrace)

}

func newError(strError string, msg string, sql string, codeError int32, trace string) *h2error {
	return &h2error{strError: strError, msg: msg, sql: sql, codeError: codeError, trace: trace}
}
func (err *h2error) Error() string {

	return fmt.Sprintf("H2 SQL Exception: [%s] %s", err.strError, err.msg)
}

func (s *session) executeQueryUpdate(stmt *h2stmt, t *transfer, values []driver.Value) (int32, error) {
	var err error
	// Check for params
	if stmt.numParams != int32(len(values)) {
		return -1, fmt.Errorf("Num expected parameters mismatch: %d != %d", stmt.numParams, len(values))
	}
	// 0. Write COMMAND EXECUTE QUERY
	log.Printf("Execute query update")
	err = t.writeInt32(sessionCommandExecuteUpdate)
	if err != nil {
		return -1, err
	}
	// 1. Write ID of query
	//st := (*stmt).(h2stmt)
	err = t.writeInt32(stmt.id)
	if err != nil {
		return -1, err
	}
	// 2. Write params
	// -- num parameters
	err = t.writeInt32(stmt.numParams)
	if err != nil {
		return -1, err
	}
	// -- parameters
	for idx, value := range values {
		switch value.(type) {
		case time.Time:
			err = t.writeDatetimeValue(value.(time.Time), stmt.parameters[idx])
		default:
			err = t.writeValue(value)
		}
		if err != nil {
			return -1, err
		}
	}
	// 3. Write Generate keys mode support
	// TODO
	err = t.writeInt32(0)
	if err != nil {
		return -1, err
	}
	err = t.flush()
	if err != nil {
		return -1, err
	}
	log.Printf("READ STATUS")
	// Read query status
	status, err := t.readInt32()
	if err != nil {
		return -1, err
	}
	// TODO: assert status == 1
	// Read num rows updated
	nUpdated, err := t.readInt32()
	if err != nil {
		return -1, err
	}
	// Read auto-commit status
	// TODO
	autoCommit, err := t.readBool()
	if err != nil {
		return -1, err
	}
	L(log.DebugLevel, "Status: %d - Num updated: %d - Autocommit: %v", status, nUpdated, autoCommit)
	return nUpdated, nil
}

func (s *session) prepare2(t *transfer, sql string, args []driver.Value) (driver.Stmt, error) {
	var err error
	stmt := h2stmt{}
	// 0. Write SESSION_PREPARE
	err = t.writeInt32(sessionPrepareReadParams2)
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

	// 4. Flush data and wait server info
	err = t.flush()
	if err != nil {
		return stmt, err
	}
	// 5. Read state
	state, err := t.readInt32()
	if err != nil {
		return stmt, err
	}
	err = s.checkSQLError(state, t)
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
	// Get command type
	cmdType, err := t.readInt32()
	if err != nil {
		return stmt, err
	}
	log.Printf("CMD type: %d", cmdType)
	// 8. Read params size
	numParams, err := t.readInt32()
	if err != nil {
		return stmt, err
	}
	log.Printf("STATE: %d, IsQuery: %v, Is Read-Only: %v, Num Params: %d", state, isQuery, isRO, numParams)
	stmt.isQuery = isQuery
	stmt.isRO = isRO
	stmt.numParams = numParams
	// We receive metadata for each parameter
	// Metadata parameter type: int:type - long:precission - int:scale - int:nullable
	for i := 0; i < int(numParams); i++ {
		param := h2parameter{}
		// -- Type
		param.kind, err = t.readInt32()
		if err != nil {
			return nil, err
		}
		// -- Precission
		param.precission, err = t.readInt64()
		if err != nil {
			return nil, err
		}
		// -- Scale
		param.scale, err = t.readInt32()
		if err != nil {
			return nil, err
		}
		// -- Nullable
		tmp, err := t.readInt32()
		if err != nil {
			return nil, err
		}
		// 0 = Not null, 1 == Nullable, 2 == Unknown
		param.nullable = tmp == 1
		log.Printf("PARAM: Kind: %d - Precission: %d - Scale: %d - Nullable: %v", param.kind, param.precission, param.scale, param.nullable)
		stmt.parameters = append(stmt.parameters, param)
	}
	return stmt, nil
}

func (s *session) close(t *transfer) error {
	var err error
	// 0. Write SESSION_CLOSE
	err = t.writeInt32(sessionClose)
	if err != nil {
		return err
	}
	err = t.flush()
	if err != nil {
		return err
	}
	// 1. Write ID
	status, err := t.readInt32()
	if err != nil {
		return err
	}
	log.Printf("Status: %d", status)
	return nil
}
