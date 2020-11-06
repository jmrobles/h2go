package h2go

import (
	"database/sql/driver"
)

type h2stmt struct {
	id        int32
	oID       int32
	isQuery   bool
	isRO      bool
	numParams int32

	driver.Stmt
}
