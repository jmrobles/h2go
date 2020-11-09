package h2go

import (
	"context"
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
	driver.Pinger
	driver.Validator
	// TODO: replace with QueryerContext instead of Queryer
	driver.QueryerContext
	driver.ExecerContext
}

// Pinger interface
func (h2c h2Conn) Ping(ctx context.Context) error {
	L(log.DebugLevel, "Ping")
	var err error
	stmt, err := h2c.client.sess.prepare(&h2c.client.trans, "SELECT 1", []driver.NamedValue{})
	if err != nil {
		return driver.ErrBadConn
	}
	st, _ := stmt.(h2stmt)
	_, _, err = h2c.client.sess.executeQuery(&st, &h2c.client.trans)
	if err != nil {
		return driver.ErrBadConn
	}
	return nil
}

// Validator interface
func (h2c h2Conn) IsValid() bool {
	// TODO: check for real valid connection
	L(log.DebugLevel, "IsValid")
	return true
}

// Conn interface
func (h2c *h2Conn) Begin() (driver.Tx, error) {
	// TODO
	return nil, nil
}
func (h2c *h2Conn) Close() error {
	L(log.DebugLevel, "Close conn")
	return h2c.client.sess.close(&h2c.client.trans)
}

func (h2c *h2Conn) Prepare(query string) (driver.Stmt, error) {
	// TODO
	return nil, nil
}

// QuerierContext interface
func (h2c *h2Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	L(log.DebugLevel, "QueryContext: %s", query)
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

func (h2c *h2Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	L(log.DebugLevel, "ExecContext: %s", query)
	var err error
	var argsValues []driver.Value
	for _, arg := range args {
		argsValues = append(argsValues, arg.Value)
	}
	stmt, err := h2c.client.sess.prepare2(&h2c.client.trans, query, argsValues)
	if err != nil {
		return nil, err
	}
	st, _ := stmt.(h2stmt)
	nUpdated, err := h2c.client.sess.executeQueryUpdate(&st, &h2c.client.trans, argsValues)
	if err != nil {
		return nil, err
	}
	return &h2ExecResult{nUpdated: nUpdated}, nil
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
