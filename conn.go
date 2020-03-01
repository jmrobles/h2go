package h2go

import (
	"database/sql/driver"
	"fmt"

	"net"

	log "github.com/sirupsen/logrus"

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
	L(log.DebugLevel, "Query: %s", query)
	var err error
	stmt, err := h2c.client.sess.prepare(&h2c.client.trans, query, args)
	if err != nil {
		return nil, err
	}
	st, _ := stmt.(h2stmt)
	cols, nRows, err := h2c.client.sess.executeQuery(&st, &h2c.client.trans)
	if err != nil {
		return nil, err
	}
	return &h2Result{query: query, columns: cols, numRows: nRows, trans: &h2c.client.trans, curRow: 0}, nil
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
