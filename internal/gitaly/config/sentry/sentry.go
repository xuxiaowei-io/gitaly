package sentry

import (
	"fmt"

	sentry "github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/middleware/panichandler"
)

// Config contains configuration for sentry
type Config struct {
	DSN         string `toml:"sentry_dsn,omitempty" json:"sentry_dsn"`
	Environment string `toml:"sentry_environment,omitempty" json:"sentry_environment"`
}

// ConfigureSentry configures the sentry DSN
func ConfigureSentry(logger logrus.FieldLogger, version string, sentryConf Config) {
	if sentryConf.DSN == "" {
		return
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:         sentryConf.DSN,
		Environment: sentryConf.Environment,
		Release:     "v" + version,
	})
	if err != nil {
		logger.Warnf("Unable to initialize sentry client: %v", err)
		return
	}

	logger.Debug("Using sentry logging")

	panichandler.InstallPanicHandler(func(grpcMethod string, _err interface{}) {
		err, ok := _err.(error)
		if !ok {
			err = fmt.Errorf("%v", _err)
		}

		sentry.WithScope(func(scope *sentry.Scope) {
			scope.SetTag("grpcMethod", grpcMethod)
			scope.SetTag("panic", "1")
			sentry.CaptureException(err)
		})
	})
}
