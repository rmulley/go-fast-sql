package batchinsert

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
) //import

func TestNewBatchInsert(t *testing.T) {
	var (
		err        error
		insertRate uint = 100
		dbh        *sql.DB
	) //var

	if dbh, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if

	bi := NewBatchInsert(dbh, insertRate)

	if bi.insertRate != insertRate {
		t.Fatal("'insertRate' not being set correctly in NewBatchInsert().")
	} //if

	if bi.values != " VALUES" {
		t.Fatal("'values' not being set correctly in NewBatchInsert().")
	} //if
} //TestNewBatchInsert

func TestSplitQuery(t *testing.T) {
	var (
		err   error
		query string
		dbh   *sql.DB
	) //var

	if dbh, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	bi := NewBatchInsert(dbh, 100)

	if err = bi.Insert(
		query,
		[]interface{}{
			1,
			2,
			3,
		},
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
