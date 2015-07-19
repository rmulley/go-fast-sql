package fastsql

import (
	"database/sql"
	"strings"
	"sync"
)

// DB is a database handle that embeds the standard library's sql.DB struct.
//
//This means the fastsql.DB struct has, and allows, access to all of the standard library functionality while also providng a superset of functionality such as batch operations, autmatically created prepared statmeents, and more.
type DB struct {
	*sql.DB
	PreparedStatements map[string]*sql.Stmt
	prepstmts          map[string]*sql.Stmt
	driverName         string
	bindParams         []interface{}
	insertCtr          uint
	flushInterval      uint
	queryPart1         string
	queryPart2         string
	values             string
} //DB

// Close is the same a sql.Close, but first closes any opened prepared statements.
func (d *DB) Close() error {
	var (
		wg sync.WaitGroup
	)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		for _, stmt := range d.PreparedStatements {
			_ = stmt.Close()
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		for _, stmt := range d.prepstmts {
			_ = stmt.Close()
		}
	}(&wg)

	wg.Wait()
	return d.DB.Close()
}

// Open is the same as sql.Open, but returns an *fastsql.DB instead.
func Open(driverName, dataSourceName string, flushInterval uint) (*DB, error) {
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
		prepstmts:          make(map[string]*sql.Stmt),
		driverName:         driverName,
		bindParams:         make([]interface{}, 0),
		flushInterval:      flushInterval,
		values:             " VALUES",
	}, err
}

// BatchInsert takes a singlular INSERT query and converts it to a batch-insert query for the caller.  A batch-insert is ran every time BatchInsert is called a multiple of flushInterval times.
func (d *DB) BatchInsert(query string, params ...interface{}) (err error) {
	// Only split out query the first time Insert is called
	if d.queryPart1 == "" {
		d.splitQuery(query)
	}

	d.insertCtr++

	// Build VALUES seciton of query and add to parameter slice
	d.values += d.queryPart2
	d.bindParams = append(d.bindParams, params...)

	// If the batch interval has been hit, execute a batch insert
	if d.insertCtr >= d.flushInterval {
		err = d.Flush()
	} //if

	return err
}

// Flush performs the acutal batch-insert query.
func (d *DB) Flush() (err error) {
	var (
		query string = d.queryPart1 + d.values[:len(d.values)-1]
	)

	// Prepare query
	if _, ok := d.prepstmts[query]; !ok {
		if stmt, err := d.DB.Prepare(query); err == nil {
			d.prepstmts[query] = stmt
		} else {
			return err
		}
	}

	// Executate batch insert
	if _, err = d.prepstmts[query].Exec(d.bindParams...); err != nil {
		return err
	} //if

	// Reset vars
	d.values = " VALUES"
	d.bindParams = make([]interface{}, 0)
	d.insertCtr = 0

	return err
}

func (d *DB) setDB(dbh *sql.DB) (err error) {
	if err = dbh.Ping(); err != nil {
		return err
	}

	d.DB = dbh
	return nil
}

func (d *DB) splitQuery(query string) {
	var (
		ndxParens, ndxValues int
	)

	// Normalize and split query
	query = strings.ToLower(query)
	ndxValues = strings.LastIndex(query, "values")
	ndxParens = strings.LastIndex(query, ")")

	// Save the first and second parts of the query separately for easier building later
	d.queryPart1 = strings.TrimSpace(query[:ndxValues])
	d.queryPart2 = query[ndxValues+6:ndxParens+1] + ","
}
