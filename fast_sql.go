package fastsql

import (
	"database/sql"
	"strings"
) //import

type DB struct {
	*sql.DB
	PreparedStatements map[string]*sql.Stmt
	driverName         string
	bindParams         []interface{}
	insertCtr          uint
	insertRate         uint
	queryPart1         string
	queryPart2         string
	values             string
} //DB

func (this *DB) Close() error {
	for _, stmt := range this.PreparedStatements {
		_ = stmt.Close()
	}

	return this.DB.Close()
}

// Open is the same as sql.Open, but returns an *fastsql.DB instead.
func Open(driverName, dataSourceName string, insertRate uint) (*DB, error) {
	var (
		err error
		dbh *sql.DB
	)

	if dbh, err = sql.Open(driverName, dataSourceName); err != nil {
		return nil, err
	}

	return &DB{
		DB:                 dbh,
		PreparedStatements: make(map[string]*sql.Stmt),
		driverName:         driverName,
		bindParams:         make([]interface{}, 0),
		insertRate:         insertRate,
		values:             " VALUES",
	}, err
}

func (this *DB) BatchInsert(query string, params ...interface{}) (err error) {
	// Only split out query the first time Insert is called
	if this.queryPart1 == "" {
		this.splitQuery(query)
	}

	this.insertCtr++

	// Build VALUES seciton of query and add to parameter slice
	this.values += this.queryPart2
	this.bindParams = append(this.bindParams, params...)

	// If the batch interval has been hit, execute a batch insert
	if this.insertCtr >= this.insertRate {
		err = this.Flush()
	} //if

	return err
}

func (this *DB) Flush() (err error) {
	var (
		query string = this.queryPart1 + this.values[:len(this.values)-1]
	)

	// Prepare query
	if _, ok := this.PreparedStatements[query]; !ok {
		if stmt, err := this.DB.Prepare(query); err == nil {
			this.PreparedStatements[query] = stmt
		} else {
			return err
		}
	}

	// Executate batch insert
	if _, err = this.PreparedStatements[query].Exec(this.bindParams...); err != nil {
		return err
	} //if

	// Reset vars
	this.values = " VALUES"
	this.bindParams = make([]interface{}, 0)
	this.insertCtr = 0

	return err
}

func (this *DB) SetDB(dbh *sql.DB) (err error) {
	if err = dbh.Ping(); err != nil {
		return err
	}

	this.DB = dbh
	return nil
}

func (this *DB) splitQuery(query string) {
	var (
		ndxParens, ndxValues int
	)

	// Normalize and split query
	query = strings.ToLower(query)
	ndxValues = strings.LastIndex(query, "values")
	ndxParens = strings.LastIndex(query, ")")

	// Save the first and second parts of the query separately for easier building later
	this.queryPart1 = strings.TrimSpace(query[:ndxValues])
	this.queryPart2 = query[ndxValues+6:ndxParens+1] + ","
}
