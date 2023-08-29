package log

import (
	"fmt"
	"io"
	"os"

	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/sirupsen/logrus"
)

const (
	// GitalyLogDirEnvKey defines the environment variable used to specify the Gitaly log directory
	GitalyLogDirEnvKey = "GITALY_LOG_DIR"
	// LogTimestampFormat defines the timestamp format in log files
	LogTimestampFormat = "2006-01-02T15:04:05.000"
	// LogTimestampFormatUTC defines the utc timestamp format in log files
	LogTimestampFormatUTC = "2006-01-02T15:04:05.000Z"
)

type utcFormatter struct {
	logrus.Formatter
}

func (u utcFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Time = e.Time.UTC()
	return u.Formatter.Format(e)
}

// UTCJsonFormatter returns a Formatter that formats a logrus Entry's as json and converts the time
// field into UTC
func UTCJsonFormatter() logrus.Formatter {
	return &utcFormatter{Formatter: &logrus.JSONFormatter{TimestampFormat: LogTimestampFormatUTC}}
}

// UTCTextFormatter returns a Formatter that formats a logrus Entry's as text and converts the time
// field into UTC
func UTCTextFormatter() logrus.Formatter {
	return &utcFormatter{Formatter: &logrus.TextFormatter{TimestampFormat: LogTimestampFormatUTC}}
}

// SkipReplacingGlobalLoggers will cause `Configure()` to skip replacing global loggers. This is mostly a hack: command
// line applications are expected to call `log.Configure()` in their subcommand actions, and that should indeed always
// replace global loggers, as well. But when running tests, we invoke the subcommand actions multiple times, which is
// thus re-configuring the logger repeatedly. Because global logger are per definition a global shared resource, the
// consequence is that we might end up replacing the global loggers while tests are using them, and this race rightfully
// gets detected by Go's race detector.
//
// This variable should thus only be set in the testhelper's setup routines such that we configure the global logger a
// single time for all of our tests, only.
var SkipReplacingGlobalLoggers bool

// Config contains logging configuration values
type Config struct {
	Dir    string `toml:"dir,omitempty" json:"dir"`
	Format string `toml:"format,omitempty" json:"format"`
	Level  string `toml:"level,omitempty" json:"level"`
}

// Configure configures the default and gRPC loggers. The gRPC logger's log level will be mapped in order to decrease
// its default verbosity. Returns the configured default logger that would also be returned by `Default()`.
func Configure(out io.Writer, format string, level string, hooks ...logrus.Hook) (logrus.FieldLogger, error) {
	logger := logrus.New() //nolint:forbidigo

	if err := configure(logger, out, format, level, hooks...); err != nil {
		return nil, fmt.Errorf("configuring logger: %w", err)
	}

	if !SkipReplacingGlobalLoggers {
		// Replace the logrus standar logger. While we shouldn't ever be using it in our own codebase, there
		// will very likely be cases where dependencies use it.
		//
		//nolint:forbidigo
		if err := configure(logrus.StandardLogger(), out, format, level, hooks...); err != nil {
			return nil, fmt.Errorf("configuring global logrus logger: %w", err)
		}

		// We replace the gRPC logger with a custom one because the default one is too chatty.
		grpcLogger := logrus.New() //nolint:forbidigo

		if err := configure(grpcLogger, out, format, mapGRPCLogLevel(level), hooks...); err != nil {
			return nil, fmt.Errorf("configuring global gRPC logger: %w", err)
		}

		grpcmwlogrus.ReplaceGrpcLogger(grpcLogger.WithField("pid", os.Getpid()))
	}

	return logger.WithField("pid", os.Getpid()), nil
}

func configure(logger *logrus.Logger, out io.Writer, format, level string, hooks ...logrus.Hook) error {
	var formatter logrus.Formatter
	switch format {
	case "json":
		formatter = UTCJsonFormatter()
	case "", "text":
		formatter = UTCTextFormatter()
	default:
		return fmt.Errorf("invalid logger format %q", format)
	}

	logrusLevel, err := logrus.ParseLevel(level)
	if err != nil {
		logrusLevel = logrus.InfoLevel
	}

	logger.Out = out
	logger.SetLevel(logrusLevel)
	logger.Formatter = formatter
	for _, hook := range hooks {
		logger.Hooks.Add(hook)
	}

	return nil
}

func mapGRPCLogLevel(level string) string {
	// Honor grpc-go's debug settings: https://github.com/grpc/grpc-go#how-to-turn-on-logging
	switch os.Getenv("GRPC_GO_LOG_SEVERITY_LEVEL") {
	case "ERROR", "error":
		return "error"
	case "WARNING", "warning":
		return "warning"
	case "INFO", "info":
		return "info"
	}

	// grpc-go is too verbose at level 'info'. So when config.toml requests
	// level info, we tell grpc-go to log at 'warn' instead.
	if level == "info" {
		return "warning"
	}

	return level
}
