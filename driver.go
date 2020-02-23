package h2go

import (
	"database/sql"
	"database/sql/driver"
)

type h2Driver struct{}

func init() {
	sql.Register("h2", &h2Driver{})
}

func (h2d *h2Driver) Open(dsn string) (driver.Conn, error) {
	ci, err := h2d.parseURL(dsn)
	if err != nil {
		return nil, err
	}
	return connect(ci)
}

func (h2d *h2Driver) parseURL(dsn string) (h2connInfo, error) {
	return h2connInfo{}, nil
}
