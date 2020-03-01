package h2go

import (
	"database/sql/driver"
	"io"

	"github.com/pkg/errors"
)

type h2Result struct {
	query   string
	columns []string
	numRows int32
	curRow  int32
	trans   *transfer
}

// Rows interface

func (h2r *h2Result) Close() error {
	return nil
}

func (h2r *h2Result) Columns() []string {
	return h2r.columns
}

func (h2r *h2Result) Next(dest []driver.Value) error {
	var err error
	// log.Printf("LEN: %d", len(dest))
	if h2r.curRow == h2r.numRows {
		return io.EOF
	}
	h2r.curRow++
	next, err := h2r.trans.readBool()
	if err != nil {
		return err
	}
	if !next {
		return io.EOF
	}
	for i := range h2r.columns {
		v, err := h2r.trans.readValue()
		if err != nil {
			return errors.Wrapf(err, "Can't read value")
		}
		dest[i] = driver.Value(v)
	}
	return nil
}
