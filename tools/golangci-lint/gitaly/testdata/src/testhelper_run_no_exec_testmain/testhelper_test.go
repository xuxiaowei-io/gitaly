package testhelper_run_no_exec_testmain

import (
	"testing"
)

func TestMain(m *testing.M) { // want "testhelper.Run not called in TestMain"
}
