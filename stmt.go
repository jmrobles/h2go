package h2go

import (
	"database/sql/driver"
)

type h2stmt struct {
	id         int32
	oID        int32
	isQuery    bool
	isRO       bool
	numParams  int32
	parameters []h2parameter

	driver.Stmt
}

type h2parameter struct {
	kind       int32
	precission int64
	scale      int32
	nullable   bool
}
