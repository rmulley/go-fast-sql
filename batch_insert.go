package batchinsert

import (
	"database/sql"
	"strings"
) //import

type BatchInsert_t struct {
	bindParams []interface{}
	dbh        *sql.DB
	insertCtr  uint
	insertRate uint
	queryPart1 string
	queryPart2 string
	values     string
} //BatchInsert_t

func NewBatchInsert(dbh *sql.DB, insertRate uint) *BatchInsert_t {
	return &BatchInsert_t{
		dbh:        dbh,
		bindParams: make([]interface{}, 0),
		insertRate: insertRate,
		values:     " VALUES",
	} //return
} //NewBatchInsert

func (this *BatchInsert_t) Insert(query string, params ...interface{}) (err error) {
	// Only split out query the first time Insert is called
	if this.queryPart1 == "" {
		var (
			ndxParens, ndxValues int
		) //var

		// Normalize and split query
		query = strings.ToLower(query)
		ndxValues = strings.LastIndex(query, "values")
		ndxParens = strings.LastIndex(query, ")")

		// Save the first and second parts of the query separately for easier building later
		this.queryPart1 = query[:ndxValues]
		this.queryPart2 = query[ndxValues+6:ndxParens+1] + ","
	} //if

	this.insertCtr++

	// Build VALUES seciton of query and add to parameter slice
	this.values += this.queryPart2
	this.bindParams = append(this.bindParams, params...)

	// If the batch interval has been hit, execute a batch insert
	if this.insertCtr >= this.insertRate {
		var (
			stmt *sql.Stmt
		) //var

		// Prepare query
		if stmt, err = this.dbh.Prepare(this.queryPart1 + this.values[:len(this.values)-1]); err != nil {
			return (err)
		} //if
		defer stmt.Close()

		// Executate batch insert
		if _, err = stmt.Exec(this.bindParams...); err != nil {
			return (err)
		} //if

		// Reset vars
		_ = stmt.Close()
		this.values = " VALUES"
		this.bindParams = make([]interface{}, 0)
		this.insertCtr = 0
	} //if

	return err
} //Insert

func (this *BatchInsert_t) Flush() (err error) {
	var (
		stmt *sql.Stmt
	) //var

	// Prepare query
	if stmt, err = this.dbh.Prepare(this.queryPart1 + this.values[:len(this.values)-1]); err != nil {
		return (err)
	} //if
	defer stmt.Close()

	// Executate batch insert
	if _, err = stmt.Exec(this.bindParams...); err != nil {
		return (err)
	} //if

	// Reset vars
	_ = stmt.Close()
	this.values = " VALUES"
	this.bindParams = make([]interface{}, 0)
	this.insertCtr = 0

	return err
} //Flush
