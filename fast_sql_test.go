package fastsql

import (
	"database/sql"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
)

func TestClose(t *testing.T) {
	var (
		err error
		dbh *DB
	)

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}

	if err = dbh.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	var (
		err           error
		flushInterval uint = 100
		dbh           *DB
	)

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbh.flushInterval != flushInterval {
		t.Fatal("'flushInterval' not being set correctly in Open().")
	}
}

func TestFlushInsert(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	)

	//t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	}
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	for i := 0; i < 3; i++ {
		if err = dbh.BatchInsert(
			query,
			[]interface{}{
				1,
				2,
				3,
			}...,
		); err != nil {
			t.Fatal(err)
		}
	}

	sqlmock.ExpectExec("insert into table_name\\(a, b, c\\) VALUES\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\)").
		WithArgs(1, 2, 3, 1, 2, 3, 1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))

	if err = dbh.FlushInsert(dbh.batchInserts[query]); err != nil {
		t.Fatal(err)
	}

	if dbh.batchInserts[query].values != " VALUES" {
		t.Fatal("dbh.values not properly reset by dbh.Flush().")
	}

	if len(dbh.batchInserts[query].bindParams) > 0 {
		t.Fatal("dbh.bindParams not properly reset by dbh.Flush().")
	}

	if dbh.batchInserts[query].insertCtr != 0 {
		t.Fatal("dbh.insertCtr not properly reset by dbh.Flush().")
	}

	// Test prepared statement error
	/*
		dbh.Close()
		if err = dbh.Flush(); err == nil {
			t.Fatal("Expecting prepared statement to fail and throw an error.")
		}
	*/
}

func TestBatchInsert(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	}
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	for i := 0; i < 3; i++ {
		if err = dbh.BatchInsert(
			query,
			[]interface{}{
				1,
				2,
				3,
			}...,
		); err != nil {
			t.Fatal(err)
		}
	}

	if len(dbh.batchInserts[query].bindParams) != 9 {
		t.Log(dbh.batchInserts[query].bindParams)
		t.Fatal("dbh.bindParams not properly set by dbh.BatchInsert().")
	}

	if dbh.batchInserts[query].insertCtr != 3 {
		t.Log(dbh.batchInserts[query].insertCtr)
		t.Fatal("dbh.insertCtr not properly being set by dbh.BatchInsert().")
	}

	if dbh.batchInserts[query].values != " VALUES(?, ?, ?),(?, ?, ?),(?, ?, ?)," {
		t.Log(dbh.batchInserts[query].values)
		t.Fatal("dbh.values not properly being set by dbh.BatchInsert().")
	}
}

func TestSetDB(t *testing.T) {
	var (
		err     error
		dbhMock *sql.DB
		dbh     *DB
	)

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	}
	defer dbhMock.Close()

	if err = dbh.setDB(dbhMock); err != nil {
		t.Fatal(err)
	}
}

func TestSplitQuery(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	)

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	}
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	if err = dbh.BatchInsert(
		query,
		[]interface{}{
			1,
			2,
			3,
		}...,
	); err != nil {
		t.Fatal(err)
	}

	if dbh.batchInserts[query].queryPart1 != "insert into table_name(a, b, c)" {
		t.Log("*" + dbh.batchInserts[query].queryPart1 + "*")
		t.Fatal("dbh.queryPart1 not formatted correctly.")
	}

	if dbh.batchInserts[query].queryPart2 != "(?, ?, ?)," {
		t.Log("*" + dbh.batchInserts[query].queryPart2 + "*")
		t.Fatal("dbh.queryPart2 not formatted correctly.")
	}
}
