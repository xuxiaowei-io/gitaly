package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/smudge"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/tracing"
)

func requireStdin(msg string) {
	var out string

	stat, err := os.Stdin.Stat()
	if err != nil {
		out = fmt.Sprintf("Cannot read from STDIN. %s (%s)", msg, err)
	} else if (stat.Mode() & os.ModeCharDevice) != 0 {
		out = fmt.Sprintf("Cannot read from STDIN. %s", msg)
	}

	if len(out) > 0 {
		fmt.Println(out)
		os.Exit(1)
	}
}

func main() {
	requireStdin("This command should be run by the Git 'smudge' filter")

	// Since the environment is sanitized at the moment, we're only
	// using this to extract the correlation ID. The finished() call
	// to clean up the tracing will be a NOP here.
	ctx, finished := tracing.ExtractFromEnv(context.Background())
	defer finished()

	logger, closer, err := configureLogging(ctx, os.Environ())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error initializing log file for gitaly-lfs-smudge: %v", err)
	}
	defer closer.Close()

	if err := run(ctx, os.Environ(), os.Stdout, os.Stdin, logger); err != nil {
		logger.WithError(err).Error(err)
		os.Exit(1)
	}
}

type nopCloser struct{}

func (nopCloser) Close() error {
	return nil
}

func configureLogging(ctx context.Context, environment []string) (logrus.FieldLogger, io.Closer, error) {
	var closer io.Closer = nopCloser{}
	writer := io.Discard

	if logDir := env.ExtractValue(environment, log.GitalyLogDirEnvKey); logDir != "" {
		logFile, err := os.OpenFile(filepath.Join(logDir, "gitaly_lfs_smudge.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, perm.SharedFile)
		if err != nil {
			// Ignore this error as we cannot do anything about it anyway. We cannot write anything to
			// stdout or stderr as that might break hooks, and we have no other destination to log to.
		} else {
			writer = logFile
		}
	}

	logger, err := log.Configure(writer, "json", "info")
	if err != nil {
		closer.Close()
		return nil, nil, err
	}

	return logger.WithField(correlation.FieldName, correlation.ExtractFromContext(ctx)), closer, nil
}

func run(ctx context.Context, environment []string, out io.Writer, in io.Reader, logger logrus.FieldLogger) error {
	cfg, err := smudge.ConfigFromEnvironment(environment)
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	switch cfg.DriverType {
	case smudge.DriverTypeFilter:
		if err := filter(ctx, cfg, out, in, logger); err != nil {
			return fmt.Errorf("running smudge filter: %w", err)
		}

		return nil
	case smudge.DriverTypeProcess:
		if err := process(ctx, cfg, out, in, logger); err != nil {
			return fmt.Errorf("running smudge process: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("unknown driver type: %v", cfg.DriverType)
	}
}
