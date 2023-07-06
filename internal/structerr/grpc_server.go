package structerr

import (
	"context"

	"github.com/sirupsen/logrus"
)

// FieldsProducer extracts metadata from err if it contains a `structerr.Error` and exposes it as
// logged fields. This function is supposed to be used with `log.MessageProducer()`.
func FieldsProducer(_ context.Context, err error) logrus.Fields {
	if metadata := ExtractMetadata(err); len(metadata) > 0 {
		return logrus.Fields{
			"error_metadata": metadata,
		}
	}

	return nil
}
