package testhelper

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
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

func newServerLogger(tb testing.TB, logName string) *logrus.Logger {
	logDir := CreateTestLogDir(tb)
	if len(logDir) == 0 {
		return NewDiscardingLogger(tb)
	}

	path := filepath.Join(logDir, logName)
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

// NewGitalyServerLogger creates a new logger. If the `TEST_LOG_DIR` environment variable is set the logger will write
// to "gitaly_server.log" inside of a test-specific subdirectory in the directory identified by the environment
// variable.
func NewGitalyServerLogger(tb testing.TB) *logrus.Logger {
	return newServerLogger(tb, "gitaly_server.log")
}

// NewPraefectServerLogger creates a new logger. If the `TEST_LOG_DIR` environment variable is set the logger will write
// to "praefect_server.log" inside of a test-specific subdirectory in the directory identified by the environment
// variable.
func NewPraefectServerLogger(tb testing.TB) *logrus.Logger {
	return newServerLogger(tb, "praefect_server.log")
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
