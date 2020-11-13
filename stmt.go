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
	"context"
	"database/sql/driver"
)

type h2stmt struct {
	id         int32
	oID        int32
	isQuery    bool
	isRO       bool
	numParams  int32
	parameters []h2parameter
	client     h2client
	query      string
	// Interfaces
	driver.Stmt
	driver.StmtQueryContext
	driver.StmtExecContext
}

type h2parameter struct {
	kind       int32
	precission int64
	scale      int32
	nullable   bool
}

// Interface Stmt
func (h2s h2stmt) Close() error {
	// TODO: check for action
	return nil
}

func (h2s h2stmt) NumInput() int {
	return int(h2s.numParams)
}

// Interface StmtQueryContext
func (h2s h2stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	cols, nRows, err := h2s.client.sess.executeQuery(&h2s, &h2s.client.trans)
	if err != nil {
		return nil, err
	}
	return &h2Result{query: h2s.query, columns: cols, numRows: nRows, trans: &h2s.client.trans, curRow: 0}, nil
}

// Interface StmtExecContext
func (h2s h2stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	var argsValues []driver.Value
	for _, arg := range args {
		argsValues = append(argsValues, arg.Value)
	}
	nUpdated, err := h2s.client.sess.executeQueryUpdate(&h2s, &h2s.client.trans, argsValues)
	if err != nil {
		return nil, err
	}
	return &h2ExecResult{nUpdated: nUpdated}, nil
}
