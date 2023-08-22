package testhelper

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
)

type loggerOptions struct {
	name string
}

// LoggerOption configures a logger.
type LoggerOption func(*loggerOptions)

// WithLoggerName sets the name of the logger. The name is included along
// the logs to help identifying the logs if multiple loggers are used.
func WithLoggerName(name string) LoggerOption {
	return func(opts *loggerOptions) {
		opts.name = name
	}
}

// NewLogger returns a logger that records the log output and
// prints it out only if the test fails.
func NewLogger(tb testing.TB, options ...LoggerOption) *logrus.Logger {
	logOutput := &bytes.Buffer{}
	logger := logrus.New() //nolint:forbidigo
	logger.Out = logOutput

	var opts loggerOptions
	for _, apply := range options {
		apply(&opts)
	}

	tb.Cleanup(func() {
		if !tb.Failed() {
			return
		}

		if opts.name != "" {
			tb.Logf("Recorded logs of %q:\n%s\n", opts.name, logOutput)
		} else {
			tb.Logf("Recorded test logs:\n%s\n", logOutput)
		}
	})

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
