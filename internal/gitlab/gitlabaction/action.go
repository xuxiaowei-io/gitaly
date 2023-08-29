// Package gitlabaction defines the actions used by the GitLab client when accessing the `/internal/allowed` endpoint.
// Due to cyclic dependencies, these actions cannot be declared in the `internal/gitlab` package directly at the time
// of writing.
package gitlabaction

// Action is an action that indicates how a specific change came to be.
type Action string

const (
	// Unknown indicates that the current action is unknown. This should in theory never be reported to the
	// client.
	Unknown = "unknown"
	// ReceivePack indicates that a change has been performed via git-receive-pack(1), or in other words by
	// a client-side push.
	ReceivePack = "git-receive-pack"
	// ResolveConflicts indicates that a change has been performed by the ResolveConflicts RPC.
	ResolveConflicts = "ResolveConflicts"
	// UserApplyPatch indicates that a change has been performed by the UserApplyPatch RPC.
	UserApplyPatch = "UserApplyPatch"
	// UserCherryPick indicates that a change has been performed by the UserCherryPick RPC.
	UserCherryPick = "UserCherryPick"
	// UserCommitFiles indicates that a change has been performed by the UserCommitFiles RPC.
	UserCommitFiles = "UserCommitFiles"
	// UserCreateBranch indicates that a change has been performed by the UserCreateBranch RPC.
	UserCreateBranch = "UserCreateBranch"
	// UserCreateTag indicates that a change has been performed by the UserCreateTag RPC.
	UserCreateTag = "UserCreateTag"
	// UserDeleteBranch indicates that a change has been performed by the UserDeleteBranch RPC.
	UserDeleteBranch = "UserDeleteBranch"
	// UserDeleteTag indicates that a change has been performed by the UserDeleteTag RPC.
	UserDeleteTag = "UserDeleteTag"
	// UserFFBranch indicates that a change has been performed by the UserFFBranch RPC.
	UserFFBranch = "UserFFBranch"
	// UserMergeBranch indicates that a change has been performed by the UserMergeBranch RPC.
	UserMergeBranch = "UserMergeBranch"
	// UserRebaseConfirmable indicates that a change has been performed by the UserRebaseConfirmable RPC.
	UserRebaseConfirmable = "UserRebaseConfirmable"
	// UserRevert indicates that a change has been performed by the UserRevert RPC.
	UserRevert = "UserRevert"
	// UserUpdateBranch indicates that a change has been performed by the UserUpdateBranch RPC.
	UserUpdateBranch = "UserUpdateBranch"
	// UserUpdateSubmodule indicates that a change has been performed by the UserUpdateSubmodule RPC.
	UserUpdateSubmodule = "UserUpdateSubmodule"
)
