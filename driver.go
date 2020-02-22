package h2go

import (
	"database/sql"
	"database/sql/driver"
	"log"
)

type h2Driver struct{}

func init() {
	sql.Register("h2", &h2Driver{})
}

func (h2d *h2Driver) Open(dsn string) (driver.Conn, error) {
	log.Printf("[H2Driver] Openning")
	return &h2Conn{}, nil
}
