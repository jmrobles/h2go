package h2go

import (
	"database/sql/driver"

	log "github.com/sirupsen/logrus"
)

type h2tx struct {
	conn h2Conn
	// Interfaces
	driver.Tx
}

// Interface Tx
func (h2t h2tx) Commit() error {
	L(log.DebugLevel, "Commit")
	stmt, err := h2t.conn.client.sess.prepare2(&h2t.conn.client.trans, "COMMIT")
	if err != nil {
		return err
	}
	st, _ := stmt.(h2stmt)
	_, err = h2t.conn.client.sess.executeQueryUpdate(&st, &h2t.conn.client.trans, []driver.Value{})
	if err != nil {
		return err
	}
	err = h2t.restoreAutocommit()
	if err != nil {
		return err
	}
	return nil
}

func (h2t h2tx) Rollback() error {
	L(log.DebugLevel, "Rollback")
	stmt, err := h2t.conn.client.sess.prepare2(&h2t.conn.client.trans, "ROLLBACK")
	if err != nil {
		return err
	}
	st, _ := stmt.(h2stmt)
	_, err = h2t.conn.client.sess.executeQueryUpdate(&st, &h2t.conn.client.trans, []driver.Value{})
	if err != nil {
		return err
	}
	err = h2t.restoreAutocommit()
	if err != nil {
		return err
	}
	return nil
}

// Helpers

func (h2t h2tx) restoreAutocommit() error {
	stmt, err := h2t.conn.client.sess.prepare2(&h2t.conn.client.trans, "SET AUTOCOMMIT TRUE")
	if err != nil {
		return err
	}
	st, _ := stmt.(h2stmt)
	_, err = h2t.conn.client.sess.executeQueryUpdate(&st, &h2t.conn.client.trans, []driver.Value{})
	if err != nil {
		return err
	}
	return nil

}
