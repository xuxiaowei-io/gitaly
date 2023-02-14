package cfgerror

import (
	"errors"
	"fmt"
	"strings"
)

// ErrNotSet should be used when the value is not set, but it is required.
var ErrNotSet = errors.New("not set")

// ValidationError represents an issue with provided configuration.
type ValidationError struct {
	// Key represents a path to the field.
	Key []string
	// Cause contains a reason why validation failed.
	Cause error
}

// Error to implement an error standard interface.
// The string representation can have 3 different formats:
// - when Key and Cause is set: "outer.inner: failure cause"
// - when only Key is set: "outer.inner"
// - when only Cause is set: "failure cause"
func (ve ValidationError) Error() string {
	if len(ve.Key) != 0 && ve.Cause != nil {
		return fmt.Sprintf("%s: %v", strings.Join(ve.Key, "."), ve.Cause)
	}
	if len(ve.Key) != 0 {
		return strings.Join(ve.Key, ".")
	}
	if ve.Cause != nil {
		return fmt.Sprintf("%v", ve.Cause)
	}
	return ""
}

// NewValidationError creates a new ValidationError with provided parameters.
func NewValidationError(err error, keys ...string) ValidationError {
	return ValidationError{Key: keys, Cause: err}
}

// ValidationErrors is a list of ValidationError-s.
type ValidationErrors []ValidationError

// Append adds provided error into current list by enriching each ValidationError with the
// provided keys or if provided err is not an instance of the ValidationError it will be wrapped
// into it. In case the nil is provided nothing happens.
func (vs ValidationErrors) Append(err error, keys ...string) ValidationErrors {
	switch terr := err.(type) {
	case nil:
		return vs
	case ValidationErrors:
		for _, err := range terr {
			vs = append(vs, ValidationError{
				Key:   append(keys, err.Key...),
				Cause: err.Cause,
			})
		}
	case ValidationError:
		vs = append(vs, ValidationError{
			Key:   append(keys, terr.Key...),
			Cause: terr.Cause,
		})
	default:
		vs = append(vs, ValidationError{
			Key:   keys,
			Cause: err,
		})
	}

	return vs
}

// AsError returns nil if there are no elements and itself if there is at least one.
func (vs ValidationErrors) AsError() error {
	if len(vs) != 0 {
		return vs
	}
	return nil
}

// Error transforms all validation errors into a single string joined by newline.
func (vs ValidationErrors) Error() string {
	var buf strings.Builder
	for i, ve := range vs {
		if i != 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(ve.Error())
	}
	return buf.String()
}

// New returns uninitialized ValidationErrors object.
func New() ValidationErrors {
	return nil
}
