// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql/driver"
	"io"

	"github.com/polytomic/odbc/api"
)

type Rows struct {
	s *Stmt
}

// implement driver.Rows
func (r *Rows) Columns() []string {
	names := make([]string, len(r.s.cols))
	for i := 0; i < len(names); i++ {
		names[i] = r.s.cols[i].Name()
	}
	return names
}

// implement driver.Rows
func (r *Rows) Next(dest []driver.Value) error {
	ret := api.SQLFetch(r.s.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLFetch", r.s.h)
	}
	for i := range dest {
		v, err := r.s.cols[i].Value(r.s.h, i)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

// implement driver.Rows
func (r *Rows) Close() error {
	r.s.rows = nil
	if !r.s.closed.Load() {
		ret := api.SQLCloseCursor(r.s.h)
		if IsError(ret) {
			return NewError("SQLCloseCursor", r.s.h)
		}
		return nil
	} else {
		return r.s.releaseHandle()
	}
}

// implement driver.RowsNextResultSet
func (r *Rows) HasNextResultSet() bool {
	return true
}

// implement driver.RowsNextResultSet
func (r *Rows) NextResultSet() error {
	ret := api.SQLMoreResults(r.s.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLMoreResults", r.s.h)
	}

	err := r.s.bindColumns()
	if err != nil {
		return err
	}
	return nil
}
