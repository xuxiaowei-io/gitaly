package testhelper

import (
	"bytes"
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

// NewGitalyServerLogger creates a logger for gitaly servers started by current test. The logger writes
// logs to a per-test log file stored in a log dir from TEST_LOG_DIR env.
func NewGitalyServerLogger(tb testing.TB) *logrus.Logger {
	logDir := CreateTestLogDir(tb)
	if len(logDir) == 0 {
		return NewDiscardingLogger(tb)
	}

	logBuffer := bytes.NewBufferString("")
	path := filepath.Join(logDir, "gitaly_server.log")
	tb.Cleanup(func() {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o755)
		require.NoError(tb, err)
		_, err = f.Write(logBuffer.Bytes())
		require.NoError(tb, err)
		require.NoError(tb, f.Close())
	})

	logger := logrus.New()
	logger.SetOutput(logBuffer)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.Infof(fmt.Sprintf("=== RUN %s", tb.Name()))

	return logger
}

// CreateTestLogDir creates a test folder from TEST_LOG_DIR environment variable if it exists
func CreateTestLogDir(tb testing.TB) string {
	testLogDir := os.Getenv("TEST_LOG_DIR")
	if len(testLogDir) == 0 {
		return ""
	}

	if _, err := os.Stat(testLogDir); os.IsNotExist(err) {
		require.NoError(tb, os.Mkdir(testLogDir, 0o755))
	}

	return testLogDir
}
