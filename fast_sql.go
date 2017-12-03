// Package fastsql is a library which extends Go's standard database/sql library.  It provides performance that's easy to take advantage of.
//
// Even better, the fastsql.DB object embeds the standard sql.DB object meaning access to all the standard database/sql library functionality is preserved.  It also means that integrating fastsql into existing codebases is a breeze.
//
// Additional functionality inclues:
//
// 1. Easy, readable, and performant batch insert queries using the BatchInsert method.
// 2. Automatic creation and re-use of prepared statements.
// 3. A convenient holder for manually used prepared statements.
package fastsql

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
)

var (
	splitQueryRegex = regexp.MustCompile(`(?i)(insert into.*?)(values\s*\(.*?)($|on\s*conflict.*)`)
)

type FastSQL interface {
	BatchInsert(query string, args ...interface{}) error
}

type fastSQL struct {
	*sql.DB
	queries map[string]Query
}

// Open is the same as sql.Open, but returns an initialized *fastSQL object instead.
func Open(driverName, dataSourceName string) (FastSQL, error) {
	dbh, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQL connection: %s", err)
	}

	return &fastSQL{
		DB: dbh,
	}, nil
}

// BatchInsert takes a singlular INSERT query and converts it to a batch-insert query for the caller.
// A batch-insert is ran every time BatchInsert is called a multiple of flushInterval times.
func (f *fastSQL) BatchInsert(queryString string, args ...interface{}) (err error) {
	queryString = strings.TrimSpace(queryString)

	var curQuery Query
	if foundQuery, ok := f.queries[queryString]; !ok {
		curQuery = newQuery(queryString)
	} else {
		curQuery = foundQuery
	}

	curQuery.Add(args)

	return nil
}

type Query interface {
	Add(args ...interface{})
}

type query struct {
	args             []interface{}
	insertClause     string
	onConflictClause string
	valuesClause     string
}

// INSERT INTO table_name(a,b,c) VALUES(d,e,f)
func newQuery(queryString string) Query {
	var q = new(query)
	q.insertClause, q.onConflictClause, q.valuesClause = splitQueryString(queryString)
	return q
}

func (q *query) Add(args ...interface{}) {
	q.args = append(q.args, args)
}

func splitQueryString(queryString string) (string, string, string) {
	var insert, conflict, values string

	// Remove starting & trailing whitespace. Removing semicolon from end of query.
	queryString = strings.TrimSpace(queryString)
	if strings.HasSuffix(queryString, ";") {
		queryString = queryString[:len(queryString)-1]
	}

	// Parse query.
	matches := splitQueryRegex.FindStringSubmatch(queryString)

	for i := 1; i < len(matches); i++ {
		switch i {
		case 1:
			log.Printf("INSERT: %s", matches[i])
			insert = strings.TrimSpace(matches[i])

		case 2:
			log.Printf("VALUES: %s", matches[i])
			values = strings.TrimSpace(matches[i])

		case 3:
			log.Printf("CONFLICT: %s", matches[i])
			conflict = strings.TrimSpace(matches[i])
		}
	}

	return insert, values, conflict
}
