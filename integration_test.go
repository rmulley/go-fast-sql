package fastsql

import (
	"log"
	"net/url"
	"os"
	"testing"
)

const (
	MYSQL_ADDR string = "127.0.0.1"
	MYSQL_DB   string = "test"
	MYSQL_USER string = "travis"
	MYSQL_PASS string = ""
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

	query = `
		CREATE TABLE test_bulk_insert (
			id tinyint(3) unsigned NOT NULL,
			id2 tinyint(3) unsigned NOT NULL,
			id3 tinyint(3) unsigned NOT NULL,
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;`

	if _, err = dbh.Exec(query); err != nil {
		log.Fatalln(err)
	}

	os.Exit(m.Run())
}

func testBulkInsert(t *testing.T) {
	var (
		i   uint = 1
		err error
	)

	// Loop performing SQL INSERTs
	for i <= 250 {
		if err = dbh.BatchInsert("INSERT INTO test_bulk_insert(id, id2, id3) VALUES(?, ?, ?);", i, i+1, i+2); err != nil {
			t.Fatal(err)
		}

		i++
	}

	if err = dbh.FlushAll(); err != nil {
		t.Fatal(err)
	}
}
