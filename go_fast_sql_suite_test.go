package fastsql_test

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

func TestGoFastSql(t *testing.T) {
	var testDir = "."

	if os.Getenv("CIRCLE_TEST_REPORTS") != "" {
		testDir = os.Getenv("CIRCLE_TEST_REPORTS")
	}

	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(testDir + "/junit.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "GoFastSql Suite", []Reporter{junitReporter})
}
