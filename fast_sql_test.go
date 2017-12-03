package fastsql

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("fastsql", func() {
	var err error

	Describe(".Open", func() {
		Context("with an unknown driver", func() {
			var dbh FastSQL

			BeforeEach(func() {
				dbh, err = Open("", "")
			})

			It("should return a nil FastSQL object", func() {
				Expect(dbh).To(BeNil())
			})

			It("should error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	}) // Describe .Open

	Describe(".splitQueryString", func() {
		Context("without target column names", func() {

		})

		Context("with target column names", func() {
			Context("with one one row to insert", func() {
				const queryString = "INSERT INTO films (code, title, did, date_prod, kind) VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama');"
				var insertClause, valuesClause, onConflictClause string

				BeforeEach(func() {
					insertClause, valuesClause, onConflictClause = splitQueryString(queryString)
				})

				It("should return a valid insert clause", func() {
					Expect(insertClause).To(Equal("INSERT INTO films (code, title, did, date_prod, kind)"))
				})

				It("should return a valid values clause", func() {
					Expect(valuesClause).To(Equal("VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama')"))
				})

				It("should return an empty on conflict clause", func() {
					Expect(onConflictClause).To(BeEmpty())
				})
			})

			Context("with multiple rows to insert", func() {
				const queryString = "INSERT INTO films (code, title, did, date_prod, kind) VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama'), ('T_602', 'Yojimbo 2', 106, '1962-07-17', 'Drama'), ('T_603', 'Yojimbo 3', 106, '1963-08-18', 'Comedy');"
				var insertClause, valuesClause, onConflictClause string

				BeforeEach(func() {
					insertClause, valuesClause, onConflictClause = splitQueryString(queryString)
				})

				It("should return a valid insert clause", func() {
					Expect(insertClause).To(Equal("INSERT INTO films (code, title, did, date_prod, kind)"))
				})

				It("should return a valid values clause", func() {
					Expect(valuesClause).To(Equal("VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama'), ('T_602', 'Yojimbo 2', 106, '1962-07-17', 'Drama'), ('T_603', 'Yojimbo 3', 106, '1963-08-18', 'Comedy')"))
				})

				It("should return an empty on conflict clause", func() {
					Expect(onConflictClause).To(BeEmpty())
				})
			})

			Context("with an 'on conclit' clause", func() {
				const queryString = "INSERT INTO distributors (did, dname) VALUES (5, 'Gizmo Transglobal'), (6, 'Associated Computing, Inc') ON CONFLICT (did) DO UPDATE SET dname = EXCLUDED.dname;"
				var insertClause, valuesClause, onConflictClause string

				BeforeEach(func() {
					insertClause, valuesClause, onConflictClause = splitQueryString(queryString)
				})

				It("should return a valid insert clause", func() {
					Expect(insertClause).To(Equal("INSERT INTO distributors (did, dname)"))
				})

				It("should return a valid values clause", func() {
					Expect(valuesClause).To(Equal("VALUES (5, 'Gizmo Transglobal'), (6, 'Associated Computing, Inc')"))
				})

				It("should return an empty on conflict clause", func() {
					Expect(onConflictClause).To(Equal("ON CONFLICT (did) DO UPDATE SET dname = EXCLUDED.dname"))
				})
			})
		})
	}) // Describe .splitQueryString
}) // Describe "fastsql"
