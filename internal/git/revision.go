package git

import (
	"bytes"
	"fmt"
)

type validateRevisionConfig struct {
	allowEmpty              bool
	allowPathScopedRevision bool
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

// AllowPathScopedRevision changes ValidateRevision to allow path-scoped revisions like
// `HEAD:README.md`. Note that path-scoped revisions may contain any character except for NUL bytes.
// Most importantly, a path-scoped revision may contain newlines.
func AllowPathScopedRevision() ValidateRevisionOption {
	return func(cfg *validateRevisionConfig) {
		cfg.allowPathScopedRevision = true
	}
}

// ValidateRevision checks if a revision looks valid. The default behaviour can be changed by
// passing ValidateRevisionOptions.
func ValidateRevision(revision []byte, opts ...ValidateRevisionOption) error {
	var cfg validateRevisionConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if bytes.HasPrefix(revision, []byte("-")) {
		return fmt.Errorf("revision can't start with '-'")
	}
	if bytes.Contains(revision, []byte("\x00")) {
		return fmt.Errorf("revision can't contain NUL")
	}

	if cfg.allowPathScopedRevision {
		// We don't need to validate the path component, if any, given that it may contain
		// all bytes except for the NUL byte which we already checked for above.
		revision, _, _ = bytes.Cut(revision, []byte(":"))
	}

	if !cfg.allowEmpty && len(revision) == 0 {
		return fmt.Errorf("empty revision")
	}
	if bytes.ContainsAny(revision, " \t\n\r") {
		return fmt.Errorf("revision can't contain whitespace")
	}
	if bytes.Contains(revision, []byte(":")) {
		return fmt.Errorf("revision can't contain ':'")
	}
	if bytes.Contains(revision, []byte("\\")) {
		return fmt.Errorf("revision can't contain '\\'")
	}

	return nil
}
