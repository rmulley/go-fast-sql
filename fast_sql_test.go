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
}) // Describe "fastsql"
