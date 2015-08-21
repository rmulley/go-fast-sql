package fastsql

import (
	"database/sql"
	"log"
	"net/url"
	"os"
	"testing"
)

const (
	INSERT_NUM_ROWS uint   = 250
	MYSQL_ADDR      string = "127.0.0.1"
	MYSQL_DB        string = "test"
	MYSQL_USER      string = "travis"
	MYSQL_PASS      string = ""
)

var (
	dbh *DB
)

func TestMain(m *testing.M) {
	var (
		err   error
		query string
	)

	// Create new FastSQL DB object with a flush-interval of 100 rows
	if dbh, err = Open("mysql", MYSQL_USER+":"+MYSQL_PASS+"@tcp("+MYSQL_ADDR+":3306)/"+MYSQL_DB+"?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		log.Fatalln(err)
	}
	defer dbh.Close()

	// Create DB table to perform INSERTs in
	query = `
		CREATE TABLE test_bulk_insert (
			id tinyint(3) unsigned NOT NULL,
			id2 tinyint(3) unsigned NOT NULL,
			id3 tinyint(3) unsigned NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;`

	if _, err = dbh.Exec(query); err != nil {
		log.Fatalln(err)
	}

	os.Exit(m.Run())
}

func testBulkInsert(t *testing.T) {
	var (
		err     error
		numRows uint
		i       uint = 1
		query   string
		row     *sql.Row
	)

	query = `
		INSERT INTO
			test_bulk_insert(id, id2, id3)
		VALUES
			(?, ?, ?);`

	// Loop performing SQL INSERTs
	for i <= INSERT_NUM_ROWS {
		if err = dbh.BatchInsert(query, i, i+1, i+2); err != nil {
			t.Fatal(err)
		}

		i++
	}

	if err = dbh.FlushAll(); err != nil {
		t.Fatal(err)
	}

	query = `
		SELECT
			COUNT(id)
		FROM
			test_bulk_insert
		WHERE
			1;`

	row = dbh.QueryRow(query)
	if err = row.Scan(numRows); err != nil {
		t.Fatal(err)
	}

	if numRows != INSERT_NUM_ROWS {
		t.Fatalf("Expected %d of rows to be inserted, %d were inserted instead.", INSERT_NUM_ROWS, numRows)
	}
}
