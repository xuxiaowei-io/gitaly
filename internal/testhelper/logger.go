package testhelper

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// NewDiscardingLogger creates a logger that discards everything.
func NewDiscardingLogger(tb testing.TB) *logrus.Logger {
	logger := logrus.New()
	logger.Out = io.Discard
	return logger
}

// NewDiscardingLogEntry creates a logrus entry that discards everything.
func NewDiscardingLogEntry(tb testing.TB) *logrus.Entry {
	return logrus.NewEntry(NewDiscardingLogger(tb))
}

// NewGitalyServerLogger creates a logger for Gitaly servers started by current test. The logger
// writes logs to gitaly_server.log inside log dir from TEST_LOG_DIR env.
func NewGitalyServerLogger(tb testing.TB) *logrus.Logger {
	logDir := CreateTestLogDir(tb)
	if len(logDir) == 0 {
		return NewDiscardingLogger(tb)
	}

	path := filepath.Join(logDir, "gitaly_server.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o755)
	require.NoError(tb, err)

	tb.Cleanup(func() { require.NoError(tb, f.Close()) })

	logger := logrus.New()
	logger.SetOutput(f)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.Infof(fmt.Sprintf("=== RUN %s", tb.Name()))

	return logger
}

// CreateTestLogDir creates the log directory specified in the `TEST_LOG_DIR` environment variable.
// If the variable is unset, then no directory will be created. This method returns the path to
// the log dir if TEST_LOG_DIR is set.
func CreateTestLogDir(tb testing.TB) string {
	testLogDir := os.Getenv("TEST_LOG_DIR")
	if len(testLogDir) == 0 {
		return ""
	}
	require.NoError(tb, os.MkdirAll(testLogDir, 0o755))

	return testLogDir
}
