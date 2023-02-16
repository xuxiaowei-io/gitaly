package testhelper

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
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
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, perm.SharedFile)
	require.NoError(tb, err)

	tb.Cleanup(func() { require.NoError(tb, f.Close()) })

	logger := logrus.New()
	logger.SetOutput(f)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.Infof(fmt.Sprintf("=== RUN %s", tb.Name()))

	return logger
}

// CreateTestLogDir creates a new log directory for testing purposes if the environment variable
// `TEST_LOG_DIR` is set. The log directory will then be created as a subdirectory of the value that
// `TEST_LOG_DIR` points to. The name of the subdirectory will match the executing test's name.
//
// Returns the name of the created log directory. If the environment variable is not set then this
// functions returns an empty string.
func CreateTestLogDir(tb testing.TB) string {
	testLogDir := os.Getenv("TEST_LOG_DIR")
	if len(testLogDir) == 0 {
		return ""
	}

	logDir := filepath.Join(testLogDir, tb.Name())

	require.NoError(tb, os.MkdirAll(logDir, perm.SharedDir))

	return logDir
}
