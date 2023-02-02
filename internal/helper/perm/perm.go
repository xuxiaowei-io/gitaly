// Package perm provides constants for file and directory permissions.
//
// Note that these permissions are further restricted by the system configured
// umask.
package perm

import "io/fs"

const (
	// PrivateDir is the permissions given for a directory that must only be
	// used by gitaly.
	PrivateDir fs.FileMode = 0o700

	// GroupPrivateDir is the permissions given for a directory that must only
	// be used by gitaly and the git group.
	GroupPrivateDir fs.FileMode = 0o770

	// SharedDir is the permission given for a directory that may be read
	// outside of gitaly.
	SharedDir fs.FileMode = 0o755

	// PublicDir is the permission given for a directory that may be read or
	// written outside of gitaly.
	PublicDir fs.FileMode = 0o777
)
