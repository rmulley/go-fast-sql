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
			const queryString = "INSERT INTO films (code, title, did, date_prod, kind) VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama');"

			Context("with one set of values", func() {
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
		})
	}) // Describe .splitQueryString
}) // Describe "fastsql"
