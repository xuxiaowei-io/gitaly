package log

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/labkit/correlation"
)

// NewHookLogger creates a file logger, since both stderr and stdout will be displayed in git output
func NewHookLogger(ctx context.Context) *logrus.Entry {
	logger := logrus.New()

	logDir := os.Getenv(GitalyLogDirEnvKey)
	if logDir == "" {
		logger.SetOutput(io.Discard)
		return logrus.NewEntry(logger)
	}

	logFile, err := os.OpenFile(filepath.Join(logDir, "gitaly_hooks.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, perm.SharedFile)
	if err != nil {
		logger.SetOutput(io.Discard)
	} else {
		logger.SetOutput(logFile)
	}

	logger.SetFormatter(UTCTextFormatter())

	return logger.WithFields(logFieldsFromContext(ctx))
}

func logFieldsFromContext(ctx context.Context) logrus.Fields {
	return logrus.Fields{
		correlation.FieldName: correlation.ExtractFromContext(ctx),
	}
}
