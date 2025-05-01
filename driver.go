// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package odbc implements database/sql driver to access data via odbc interface.
package odbc

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/polytomic/odbc/api"
)

var Recovery func() = func() {
	if r := recover(); r != nil {
		panic(fmt.Sprintf("internal failure: %v", r))
	}
}

// Driver implements database/sql/driver.Driver interface.
type Driver struct {
	Stats
	h       api.SQLHENV // environment handle
	initErr error
	Loc     *time.Location
}

// NewDriver creates a new ODBC driver instance with its own environment handle.
func NewDriver() (*Driver, error) {
	d := &Driver{}

	//Allocate environment handle
	var out api.SQLHANDLE
	in := api.SQLHANDLE(api.SQL_NULL_HANDLE)
	ret := api.SQLAllocHandle(api.SQL_HANDLE_ENV, in, &out)
	if IsError(ret) {
		return nil, NewError("SQLAllocHandle", api.SQLHENV(in))
	}
	d.h = api.SQLHENV(out)

	// will use ODBC v3
	ret = api.SQLSetEnvUIntPtrAttr(d.h, api.SQL_ATTR_ODBC_VERSION, api.SQL_OV_ODBC3, 0)
	if IsError(ret) {
		defer releaseHandle(d.h)
		return nil, NewError("SQLSetEnvUIntPtrAttr", d.h)
	}

	//Enable connection pooling
	ret = api.SQLSetEnvUIntPtrAttr(d.h, api.SQL_ATTR_CONNECTION_POOLING, api.SQL_CP_ONE_PER_HENV, api.SQL_IS_UINTEGER)
	if IsError(ret) {
		defer releaseHandle(d.h)
		return nil, NewError("SQLSetEnvUIntPtrAttr", d.h)
	}

	//Set relaxed connection pool matching
	ret = api.SQLSetEnvUIntPtrAttr(d.h, api.SQL_ATTR_CP_MATCH, api.SQL_CP_RELAXED_MATCH, api.SQL_IS_UINTEGER)
	if IsError(ret) {
		defer releaseHandle(d.h)
		return nil, NewError("SQLSetEnvUIntPtrAttr", d.h)
	}

	return d, nil
}

func (d *Driver) Close() error {
	h := d.h
	d.h = api.SQLHENV(api.SQL_NULL_HENV)
	return releaseHandle(h)
}

// driverFactory creates a new driver instance for each connection
type driverFactory struct{}

func (df *driverFactory) Open(name string) (driver.Conn, error) {
	// Create a new driver for this connection
	drv, err := NewDriver()
	if err != nil {
		return nil, err
	}

	// Use the new driver to open a connection
	conn, err := drv.Open(name)
	if err != nil {
		drv.Close()
		return nil, err
	}

	return conn, nil
}

func init() {
	sql.Register("odbc", &driverFactory{})
}
