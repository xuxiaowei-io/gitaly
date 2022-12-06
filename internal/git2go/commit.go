package git2go

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// UnknownIndexError is an unspecified error that was produced by performing an invalid operation on the index.
type UnknownIndexError string

// Error returns the error message of the unknown index error.
func (err UnknownIndexError) Error() string { return string(err) }

// IndexErrorType specifies which of the known index error types has occurred.
type IndexErrorType uint

const (
	// ErrDirectoryExists represent a directory exists error.
	ErrDirectoryExists IndexErrorType = iota
	// ErrDirectoryTraversal represent a directory traversal error.
	ErrDirectoryTraversal
	// ErrEmptyPath represent an empty path error.
	ErrEmptyPath
	// ErrFileExists represent a file exists error.
	ErrFileExists
	// ErrFileNotFound represent a file not found error.
	ErrFileNotFound
	// ErrInvalidPath represent an invalid path error.
	ErrInvalidPath
)

// IndexError is a well-defined error that was produced by performing an invalid operation on the index.
type IndexError struct {
	Path string
	Type IndexErrorType
}

// Error returns the error message associated with the error type.
func (err IndexError) Error() string {
	switch err.Type {
	case ErrDirectoryExists:
		return "A directory with this name already exists"
	case ErrDirectoryTraversal:
		return "Path cannot include directory traversal"
	case ErrEmptyPath:
		return "You must provide a file path"
	case ErrFileExists:
		return "A file with this name already exists"
	case ErrFileNotFound:
		return "A file with this name doesn't exist"
	case ErrInvalidPath:
		return fmt.Sprintf("invalid path: '%s'", err.Path)
	default:
		panic(fmt.Sprintf("unhandled IndexErrorType: %v", err.Type))
	}
}

// Proto returns the Protobuf representation of this error.
func (err IndexError) Proto() *gitalypb.IndexError {
	errType := gitalypb.IndexError_ERROR_TYPE_UNSPECIFIED
	switch err.Type {
	case ErrDirectoryExists:
		errType = gitalypb.IndexError_ERROR_TYPE_DIRECTORY_EXISTS
	case ErrDirectoryTraversal:
		errType = gitalypb.IndexError_ERROR_TYPE_DIRECTORY_TRAVERSAL
	case ErrEmptyPath:
		errType = gitalypb.IndexError_ERROR_TYPE_EMPTY_PATH
	case ErrFileExists:
		errType = gitalypb.IndexError_ERROR_TYPE_FILE_EXISTS
	case ErrFileNotFound:
		errType = gitalypb.IndexError_ERROR_TYPE_FILE_NOT_FOUND
	case ErrInvalidPath:
		errType = gitalypb.IndexError_ERROR_TYPE_INVALID_PATH
	}

	return &gitalypb.IndexError{
		Path:      []byte(err.Path),
		ErrorType: errType,
	}
}

// StructuredError returns the structured error.
func (err IndexError) StructuredError() structerr.Error {
	e := errors.New(err.Error())
	switch err.Type {
	case ErrDirectoryExists, ErrFileExists:
		return structerr.NewAlreadyExists("%w", e)
	case ErrDirectoryTraversal, ErrEmptyPath, ErrInvalidPath:
		return structerr.NewInvalidArgument("%w", e)
	case ErrFileNotFound:
		return structerr.NewNotFound("%w", e)
	default:
		return structerr.NewInternal("%w", e)
	}
}

// InvalidArgumentError is returned when an invalid argument is provided.
type InvalidArgumentError string

func (err InvalidArgumentError) Error() string { return string(err) }

// CommitCommand contains the information and the steps to build a commit.
type CommitCommand struct {
	// Repository is the path of the repository to operate on.
	Repository string
	// Author is the author of the commit.
	Author Signature
	// Committer is the committer of the commit.
	Committer Signature
	// Message is message of the commit.
	Message string
	// Parent is the OID of the commit to use as the parent of this commit.
	Parent string
	// Actions are the steps to build the commit.
	Actions []Action
	// SigningKey is a path to the key to sign commit using OpenPGP
	SigningKey string
}

// Commit builds a commit from the actions, writes it to the object database and
// returns its object id.
func (b *Executor) Commit(ctx context.Context, repo repository.GitRepo, c CommitCommand) (git.ObjectID, error) {
	c.SigningKey = b.signingKey

	return b.runWithGob(ctx, repo, "commit", c)
}
