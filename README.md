[![Build Status](https://travis-ci.org/rmulley/go-fast-sql.png)](https://travis-ci.org/rmulley/go-fast-sql)
[![Coverage Status](https://coveralls.io/repos/rmulley/go-fast-sql/badge.svg?branch=master)](https://coveralls.io/r/rmulley/go-fast-sql?branch=master)
# go-fast-sql
A Golang library designed to speed up SQL queries by batching INSERTs, UPDATEs, and DELETEs.  It's designed to be used in a manor very similar to Go's built-in [database/sql](http://golang.org/pkg/database/sql/) package.

##Example usage

```go
package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rmulley/go-fast-sql"
	"log"
	"net/url"
) //import

func main() {
	var (
		err error
		i   uint = 1
		dbh *sql.DB
		bi  *batchinsert.BatchInsert_t
	) //var

	// Set up DB conn using Go's built-in database/sql pkg and go-sql-driver's MySQL driver
	if dbh, err = sql.Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York")); err != nil {
		log.Fatalln(err)
	} //if
	defer dbh.Close()

	// Create new BatchInsert object that will run a batch SQL INSERT every 100 rows
	bi = batchinsert.NewBatchInsert(dbh, 100)

	// Some loop performing SQL INSERTs
	for i <= 250 {
		if err = bi.Insert("INSERT INTO test_table(id, id2, id3) VALUES(?, ?, ?);", i, i + 1, i + 2); err != nil {
			log.Fatalln(err)
		} //if

		i++
	} //for

	// Flush out remaining insert (Last 50 rows)
	if err = bi.Flush(); err != nil {
		log.Fatalln(err)
	} //if
} //main
```
