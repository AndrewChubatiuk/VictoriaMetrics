package notifier

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	tmpl, _ := LoadTemplates([]string{"testdata/*good.tmpl"})
	SetTemplate(tmpl)
	os.Exit(m.Run())
}
