package log

import "github.com/sirupsen/logrus"

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
