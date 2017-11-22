package fastsql_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGoFastSql(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoFastSql Suite")
}
