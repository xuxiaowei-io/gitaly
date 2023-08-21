// Package gitlabaction defines the actions used by the GitLab client when accessing the `/internal/allowed` endpoint.
// Due to cyclic dependencies, these actions cannot be declared in the `internal/gitlab` package directly at the time
// of writing.
package gitlabaction

// Action is an action that indicates how a specific change came to be.
type Action string

const (
	// ReceivePack indicates that a change has been performed via git-receive-pack(1), or in other words by
	// a client-side push.
	ReceivePack = "git-receive-pack"
)
