package log

import (
	"context"

	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Fields contains key-value pairs of structured logging data.
type Fields = logrus.Fields

// Logger is the logging type used by Gitaly.
type Logger interface {
	WithField(key string, value any) Logger
	WithFields(fields Fields) Logger
	WithError(err error) Logger

	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
	Panicf(format string, args ...any)

	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
	Panic(args ...any)

	Debugln(args ...any)
	Infoln(args ...any)
	Warnln(args ...any)
	Errorln(args ...any)
	Panicln(args ...any)

	StreamServerInterceptor(...grpcmwlogrus.Option) grpc.StreamServerInterceptor
	UnaryServerInterceptor(...grpcmwlogrus.Option) grpc.UnaryServerInterceptor
}

// LogrusLogger is an implementation of the Logger interface that is implemented via a `logrus.FieldLogger`.
type LogrusLogger struct {
	*logrus.Entry
}

// FromLogrusEntry constructs a new Gitaly-specific logger from a `logrus.Logger`.
func FromLogrusEntry(entry *logrus.Entry) LogrusLogger {
	return LogrusLogger{Entry: entry}
}

// WithField creates a new logger with the given field appended.
func (l LogrusLogger) WithField(key string, value any) Logger {
	return LogrusLogger{Entry: l.Entry.WithField(key, value)}
}

// WithFields creates a new logger with the given fields appended.
func (l LogrusLogger) WithFields(fields Fields) Logger {
	return LogrusLogger{Entry: l.Entry.WithFields(fields)}
}

// WithError creates a new logger with an appended error field.
func (l LogrusLogger) WithError(err error) Logger {
	return LogrusLogger{Entry: l.Entry.WithError(err)}
}

// ToContext injects the logger into the given context so that it can be retrieved via `FromContext()`.
func (l LogrusLogger) ToContext(ctx context.Context) context.Context {
	return ctxlogrus.ToContext(ctx, l.Entry)
}

// StreamServerInterceptor creates a gRPC interceptor that generates log messages for streaming RPC calls.
func (l LogrusLogger) StreamServerInterceptor(opts ...grpcmwlogrus.Option) grpc.StreamServerInterceptor {
	return grpcmwlogrus.StreamServerInterceptor(l.Entry, opts...)
}

// UnaryServerInterceptor creates a gRPC interceptor that generates log messages for unary RPC calls.
func (l LogrusLogger) UnaryServerInterceptor(opts ...grpcmwlogrus.Option) grpc.UnaryServerInterceptor {
	return grpcmwlogrus.UnaryServerInterceptor(l.Entry, opts...)
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
func AddFields(ctx context.Context, fields Fields) {
	ctxlogrus.AddFields(ctx, fields)
}
