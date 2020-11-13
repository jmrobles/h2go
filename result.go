/*
Copyright 2020 JM Robles (@jmrobles)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	// Interface
	driver.Rows
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
	// log.Printf(">>> DEST: %v", dest)
	for i := range h2r.columns {
		v, err := h2r.trans.readValue()
		if err != nil {
			return errors.Wrapf(err, "Can't read value")
		}
		dest[i] = driver.Value(v)
	}
	return nil
}

type h2ExecResult struct {
	nUpdated int32
	// Interface
	driver.Result
}

func (h2er *h2ExecResult) LastInsertId() (int64, error) {
	return 1, nil
}

func (h2er *h2ExecResult) RowsAffected() (int64, error) {
	return int64(h2er.nUpdated), nil
}
