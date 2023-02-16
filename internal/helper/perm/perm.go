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

	// PrivateWriteOnceFile is the most restrictive file permission. Given to
	// files that are expected to be written only once and must be read only by
	// gitaly.
	PrivateWriteOnceFile fs.FileMode = 0o400

	// PrivateFile is the permissions given for a file that must only be used
	// by gitaly.
	PrivateFile fs.FileMode = 0o600

	// SharedFile is the permission given for a file that may be read outside
	// of gitaly.
	SharedFile fs.FileMode = 0o644

	// PublicFile is the permission given for a file that may be read or
	// written outside of gitaly.
	PublicFile fs.FileMode = 0o666

	// PrivateExecutable is the permissions given for an executable that must
	// only be used by gitaly.
	PrivateExecutable fs.FileMode = 0o700

	// SharedExecutable is the permission given for an executable that may be
	// executed outside of gitaly.
	SharedExecutable fs.FileMode = 0o755
)
