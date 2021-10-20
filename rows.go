// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql/driver"
	"io"
	"reflect"

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
	if r.s.c.closingInBG.Load() {
		//if we are cancelling/closing in a background thread, ignore requests to Close this statement from the driver
		return nil
	}
	r.s.rows = nil
	if ret := api.SQLCloseCursor(r.s.h); IsError(ret) {
		return NewError("SQLCloseCursor", r.s.h)
	}
	return nil
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

// ColumnTypeScanType should return the value type that can be used to scan
// types into.
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	return r.s.cols[index].ScanType()
}

// Nullable returns true if the column is nullable and false otherwise.
// If the column nullability is unknown, ok is false.
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.s.cols[index].Nullable()
}

// ColumnTypeDatabaseTypeName return the database system type name.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	switch x := r.s.cols[index].(type) {
	case *BindableColumn:
		return cTypeString(x.CType)
	case *NonBindableColumn:
		return cTypeString(x.CType)
	}
	return ""
}

func cTypeString(ct api.SQLSMALLINT) string {
	switch ct {
	case api.SQL_C_CHAR:
		return "SQL_C_CHAR"
	case api.SQL_C_LONG:
		return "SQL_C_LONG"
	case api.SQL_C_SHORT:
		return "SQL_C_SHORT"
	case api.SQL_C_FLOAT:
		return "SQL_C_FLOAT"
	case api.SQL_C_DOUBLE:
		return "SQL_C_DOUBLE"
	case api.SQL_C_NUMERIC:
		return "SQL_C_NUMERIC"
	case api.SQL_C_DATE:
		return "SQL_C_DATE"
	case api.SQL_C_TIME:
		return "SQL_C_TIME"
	case api.SQL_C_TYPE_TIMESTAMP:
		return "SQL_C_TYPE_TIMESTAMP"
	case api.SQL_C_TIMESTAMP:
		return "SQL_C_TIMESTAMP"
	case api.SQL_C_BINARY:
		return "SQL_C_BINARY"
	case api.SQL_C_BIT:
		return "SQL_C_BIT"
	case api.SQL_C_WCHAR:
		return "SQL_C_WCHAR"
	case api.SQL_C_DEFAULT:
		return "SQL_C_DEFAULT"
	case api.SQL_C_SBIGINT:
		return "SQL_C_SBIGINT"
	case api.SQL_C_UBIGINT:
		return "SQL_C_UBIGINT"
	case api.SQL_C_GUID:
		return "SQL_C_GUID"
	}
	return ""
}
