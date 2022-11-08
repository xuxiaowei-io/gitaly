package errors

import "errors"

var (
	// ErrEmptyRepository is returned when an RPC is missing a repository as an argument
	ErrEmptyRepository = errors.New("empty Repository")
	// ErrEmptyStorageName is returned when an RPC is missing a storage name for the repository.
	ErrEmptyStorageName = errors.New("empty StorageName")
	// ErrEmptyRelativePath is returned when an RPC is missing a relative path for the repository.
	ErrEmptyRelativePath = errors.New("empty RelativePath")
	// ErrInvalidRepository is returned when an RPC has an invalid repository as an argument
	ErrInvalidRepository = errors.New("invalid Repository")
)
