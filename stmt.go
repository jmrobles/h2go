package h2go

import (
	"database/sql/driver"
)

type h2stmt struct {
	id int32

	driver.Stmt
}
