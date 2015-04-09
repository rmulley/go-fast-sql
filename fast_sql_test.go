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

	fi := NewFastSQL(dbh, insertRate)

	if fi.insertRate != insertRate {
		t.Fatal("'insertRate' not being set correctly in NewFastSQL().")
	} //if

	if fi.values != " VALUES" {
		t.Fatal("'values' not being set correctly in NewFastSQL().")
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

	fi := NewFastSQL(dbh, 100)

	for i := 0; i < 3; i++ {
		if err = fi.Insert(
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

	if err = fi.Flush(); err != nil {
		t.Fatal(err)
	} //if

	if fi.values != " VALUES" {
		t.Fatal("fi.values not properly reset by fi.Flush().")
	} //if

	if len(fi.bindParams) > 0 {
		t.Fatal("fi.bindParams not properly reset by fi.Flush().")
	} //if

	if fi.insertCtr != 0 {
		t.Fatal("fi.insertCtr not properly reset by fi.Flush().")
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

	fi := NewFastSQL(dbh, 100)

	for i := 0; i < 3; i++ {
		if err = fi.Insert(
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

	if len(fi.bindParams) != 9 {
		t.Log(fi.bindParams)
		t.Fatal("fi.bindParams not properly set by fi.Insert().")
	} //if

	if fi.insertCtr != 3 {
		t.Log(fi.insertCtr)
		t.Fatal("fi.insertCtr not properly being set by fi.Insert().")
	} //if

	if fi.values != " VALUES(?, ?, ?),(?, ?, ?),(?, ?, ?)," {
		t.Log(fi.values)
		t.Fatal("fi.values not properly being set by fi.Insert().")
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

	fi := NewFastSQL(dbh, 100)

	if err = fi.Insert(
		query,
		[]interface{}{
			1,
			2,
			3,
		}...,
	); err != nil {
		t.Fatal(err)
	} //if

	if fi.queryPart1 != "insert into table_name(a, b, c)" {
		t.Log("*" + fi.queryPart1 + "*")
		t.Fatal("fi.queryPart1 not formatted correctly.")
	} //if

	if fi.queryPart2 != "(?, ?, ?)," {
		t.Log("*" + fi.queryPart2 + "*")
		t.Fatal("fi.queryPart2 not formatted correctly.")
	} //if
} //TestSplitQuery
