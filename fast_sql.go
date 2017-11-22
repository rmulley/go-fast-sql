// Package fastsql is a library which extends Go's standard database/sql library.  It provides performance that's easy to take advantage of.
//
// Even better, the fastsql.DB object embeds the standard sql.DB object meaning access to all the standard database/sql library functionality is preserved.  It also means that integrating fastsql into existing codebases is a breeze.
//
// Additional functionality inclues:
//
// 1. Easy, readable, and performant batch insert queries using the BatchInsert method.
// 2. Automatic creation and re-use of prepared statements.
// 3. A convenient holder for manually used prepared statements.
package fastsql

import (
	"database/sql"
	"fmt"
)

type FastSQL interface {
	BatchInsert(query string, args ...interface{}) error
}

type fastSQL struct {
	*sql.DB
}

// Open is the same as sql.Open, but returns an initialized *fastSQL object instead.
func Open(driverName, dataSourceName string) (FastSQL, error) {
	dbh, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQL connection: %s", err)
	}

	return &fastSQL{
		DB: dbh,
	}, nil
}

// BatchInsert takes a singlular INSERT query and converts it to a batch-insert query for the caller.
// A batch-insert is ran every time BatchInsert is called a multiple of flushInterval times.
func (f *fastSQL) BatchInsert(query string, args ...interface{}) (err error) {

	return nil
}
