package git2go

import "gitlab.com/gitlab-org/gitaly/v16/internal/git"

// Signature represents a commits signature, synced from internal/git.
type Signature = git.Signature

// NewSignature creates a new sanitized signature, syned from internal/git.
var NewSignature = git.NewSignature
