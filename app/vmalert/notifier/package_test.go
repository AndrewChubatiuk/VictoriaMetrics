package notifier

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	LoadTemplates([]string{"testdata/templates/*good.tmpl"}, true)
	os.Exit(m.Run())
}
