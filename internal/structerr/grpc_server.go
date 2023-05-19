package structerr

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
)

// FieldsProducer extracts metadata from err if it contains a `structerr.Error` and exposes it as
// logged fields. This function is supposed to be used with `log.MessageProducer()`.
func FieldsProducer(_ context.Context, err error) logrus.Fields {
	var structErr Error
	if errors.As(err, &structErr) {
		metadata := structErr.Metadata()
		if len(metadata) == 0 {
			return nil
		}

		return logrus.Fields{
			"error_metadata": metadata,
		}
	}

	return nil
}
