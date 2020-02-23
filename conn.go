package h2go

import (
	"database/sql/driver"
	"log"
)

type h2Conn struct {
	driver.Conn
}
type h2Result struct {
	query string
}

type h2connInfo struct{}

// Conn interface
func (h2c *h2Conn) Begin() (driver.Tx, error) {
	return nil, nil
}
func (h2c *h2Conn) Close() error {
	return nil
}

func (h2c *h2Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

// Querier interface
func (h2c *h2Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	log.Printf("Query: %s", query)
	return &h2Result{query}, nil
}

// Rows interface

func (h2r *h2Result) Close() error {
	return nil
}

func (h2r *h2Result) Columns() []string {
	return []string{"jander", "sander"}
}

func (h2r *h2Result) Next(dest []driver.Value) error {
	return nil
}

// Specific code

func connect(ci h2connInfo) (driver.Conn, error) {

	return &h2Conn{}, nil
}
