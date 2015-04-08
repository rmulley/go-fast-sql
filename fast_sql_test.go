package fastsql

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
) //import

func TestNewFastSQL(t *testing.T) {
	var (
		err        error
		insertRate uint = 100
		dbh        *sql.DB
	) //var

	t.Parallel()

	if dbh, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if

	bi := NewFastSQL(dbh, insertRate)

	if bi.insertRate != insertRate {
		t.Fatal("'insertRate' not being set correctly in NewBatchInsert().")
	} //if

	if bi.values != " VALUES" {
		t.Fatal("'values' not being set correctly in NewBatchInsert().")
	} //if
} //TestNewFastSQL

func TestFlush(t *testing.T) {
	var (
		err   error
		query string
		dbh   *sql.DB
	) //var

	t.Parallel()

	if dbh, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	bi := NewFastSQL(dbh, 100)

	for i := 0; i < 3; i++ {
		if err = bi.Insert(
			query,
			[]interface{}{
				1,
				2,
				3,
			}...,
		); err != nil {
			t.Fatal(err)
		} //if
	} //for

	sqlmock.ExpectExec("insert into table_name\\(a, b, c\\) VALUES\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\)").
		WithArgs(1, 2, 3, 1, 2, 3, 1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))

	if err = bi.Flush(); err != nil {
		t.Fatal(err)
	} //if

	if bi.values != " VALUES" {
		t.Fatal("bi.values not properly reset by bi.Flush().")
	} //if

	if len(bi.bindParams) > 0 {
		t.Fatal("bi.bindParams not properly reset by bi.Flush().")
	} //if

	if bi.insertCtr != 0 {
		t.Fatal("bi.insertCtr not properly reset by bi.Flush().")
	} //if
} //TestFlush

func TestInsert(t *testing.T) {
	var (
		err   error
		query string
		dbh   *sql.DB
	) //var

	t.Parallel()

	if dbh, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	bi := NewFastSQL(dbh, 100)

	for i := 0; i < 3; i++ {
		if err = bi.Insert(
			query,
			[]interface{}{
				1,
				2,
				3,
			}...,
		); err != nil {
			t.Fatal(err)
		} //if
	} //for

	if len(bi.bindParams) != 9 {
		t.Log(bi.bindParams)
		t.Fatal("bi.bindParams not properly set by bi.Insert().")
	} //if

	if bi.insertCtr != 3 {
		t.Log(bi.insertCtr)
		t.Fatal("bi.insertCtr not properly being set by bi.Insert().")
	} //if

	if bi.values != " VALUES(?, ?, ?),(?, ?, ?),(?, ?, ?)," {
		t.Log(bi.values)
		t.Fatal("bi.values not properly being set by bi.Insert().")
	} //if
} //TestInsert

func TestSplitQuery(t *testing.T) {
	var (
		err   error
		query string
		dbh   *sql.DB
	) //var

	t.Parallel()

	if dbh, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	bi := NewFastSQL(dbh, 100)

	if err = bi.Insert(
		query,
		[]interface{}{
			1,
			2,
			3,
		}...,
	); err != nil {
		t.Fatal(err)
	} //if

	if bi.queryPart1 != "insert into table_name(a, b, c)" {
		t.Log("*" + bi.queryPart1 + "*")
		t.Fatal("bi.queryPart1 not formatted correctly.")
	} //if

	if bi.queryPart2 != "(?, ?, ?)," {
		t.Log("*" + bi.queryPart2 + "*")
		t.Fatal("bi.queryPart2 not formatted correctly.")
	} //if
} //TestSplitQuery
