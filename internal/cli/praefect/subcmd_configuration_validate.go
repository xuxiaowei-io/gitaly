package praefect

import (
	"io"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/cmd"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/config"
)

const (
	configurationCmdName = "configuration"
	validateCmdName      = "validate"
	validationErrorCode  = 2
)

// validateConfiguration checks if provided configuration is valid.
func validateConfiguration(reader io.Reader, outWriter, errWriter io.Writer) int {
	logrus.SetLevel(logrus.ErrorLevel)

	cfg, err := config.FromReader(reader)
	if err != nil {
		if cmd.WriteTomlReadError(err, outWriter, errWriter) {
			return validationErrorCode
		}
		return 1
	}

	if !cmd.Validate(&cfg, outWriter, errWriter) {
		return validationErrorCode
	}

	return 0
}
