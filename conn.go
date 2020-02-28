package h2go

import (
	"database/sql/driver"
	"fmt"
	"log"
	"net"

	"github.com/pkg/errors"
)

const defaultH2port = 9092

type h2Conn struct {
	connInfo h2connInfo
	client   h2client
	// Interfaces
	driver.Conn
	driver.Queryer
}
type h2Result struct {
	query string
}

type h2connInfo struct {
	host     string
	port     int
	database string
	username string
	password string
	isMem    bool
	// client   h2client

	dialer net.Dialer
}

type h2client struct {
	conn  net.Conn
	trans transfer
	sess  session
}

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
	err = h2c.client.sess.prepare(h2c.client.trans, args)
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
	var conn net.Conn
	var err error
	address := fmt.Sprintf("%s:%d", ci.host, ci.port)
	conn, err = ci.dialer.Dial("tcp", address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open H2 connection")
	}
	t := newTransfer(conn)
	c := h2client{conn: conn, trans: t, sess: newSession()}
	err = c.doHandshake(ci)
	if err != nil {
		return nil, errors.Wrapf(err, "error doing H2 server handshake")
	}
	// ci.client = c
	return &h2Conn{connInfo: ci, client: c}, nil
}
