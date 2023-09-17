package log

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
)

// Logger is the logging type used by Gitaly.
type Logger interface {
	logrus.FieldLogger
}

// LogrusLogger is an implementation of the Logger interface that is implemented via a `logrus.FieldLogger`.
type LogrusLogger struct {
	*logrus.Entry
}

// FromLogrusEntry constructs a new Gitaly-specific logger from a `logrus.Logger`.
func FromLogrusEntry(entry *logrus.Entry) LogrusLogger {
	return LogrusLogger{Entry: entry}
}

// ToContext injects the logger into the given context so that it can be retrieved via `FromContext()`.
func (l LogrusLogger) ToContext(ctx context.Context) context.Context {
	return ctxlogrus.ToContext(ctx, l.Entry)
}

// FromContext extracts the logger from the context. If no logger has been injected then this will return a discarding
// logger.
func FromContext(ctx context.Context) LogrusLogger {
	return LogrusLogger{
		Entry: ctxlogrus.Extract(ctx),
	}
}

// AddFields adds the given log fields to the context so that it will be used by any context logger extracted via
// `FromContext()`.
func AddFields(ctx context.Context, fields logrus.Fields) {
	ctxlogrus.AddFields(ctx, fields)
}
