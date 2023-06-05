package git

import (
	"bytes"
	"fmt"
)

type validateRevisionConfig struct {
	allowEmpty bool
}

// ValidateRevisionOption is an option that can be passed to ValidateRevision.
type ValidateRevisionOption func(cfg *validateRevisionConfig)

// AllowEmptyRevision changes ValidateRevision to not return an error in case the specified
// revision is empty.
func AllowEmptyRevision() ValidateRevisionOption {
	return func(cfg *validateRevisionConfig) {
		cfg.allowEmpty = true
	}
}

// ValidateRevision checks if a revision looks valid. The default behaviour can be changed by
// passing ValidateRevisionOptions.
func ValidateRevision(revision []byte, opts ...ValidateRevisionOption) error {
	var cfg validateRevisionConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if !cfg.allowEmpty && len(revision) == 0 {
		return fmt.Errorf("empty revision")
	}
	if bytes.HasPrefix(revision, []byte("-")) {
		return fmt.Errorf("revision can't start with '-'")
	}
	if bytes.ContainsAny(revision, " \t\n\r") {
		return fmt.Errorf("revision can't contain whitespace")
	}
	if bytes.Contains(revision, []byte("\x00")) {
		return fmt.Errorf("revision can't contain NUL")
	}
	if bytes.Contains(revision, []byte(":")) {
		return fmt.Errorf("revision can't contain ':'")
	}
	if bytes.Contains(revision, []byte("\\")) {
		return fmt.Errorf("revision can't contain '\\'")
	}

	return nil
}
