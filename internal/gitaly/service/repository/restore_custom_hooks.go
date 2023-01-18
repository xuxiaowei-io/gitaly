package repository

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

// RestoreCustomHooks sets the git hooks for a repository. The hooks are sent in
// a tar archive containing a `custom_hooks` directory. This directory is
// ultimately extracted to the repository.
func (s *server) RestoreCustomHooks(stream gitalypb.RepositoryService_RestoreCustomHooksServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return structerr.NewInternal("first request failed %w", err)
	}

	repo := firstRequest.GetRepository()
	if err := service.ValidateRepository(repo); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		if firstRequest != nil {
			data := firstRequest.GetData()
			firstRequest = nil
			return data, nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	if featureflag.TransactionalRestoreCustomHooks.IsEnabled(ctx) {
		if err := s.restoreCustomHooks(ctx, reader, repo); err != nil {
			return structerr.NewInternal("setting custom hooks: %w", err)
		}

		return stream.SendAndClose(&gitalypb.RestoreCustomHooksResponse{})
	}

	repoPath, err := s.locator.GetPath(repo)
	if err != nil {
		return structerr.NewInternal("getting repo path failed %w", err)
	}

	if err := extractHooks(ctx, reader, repoPath); err != nil {
		return structerr.NewInternal("extracting hooks: %w", err)
	}

	return stream.SendAndClose(&gitalypb.RestoreCustomHooksResponse{})
}

// restoreCustomHooks transactionally and atomically sets the provided custom
// hooks for the specified repository.
func (s *server) restoreCustomHooks(ctx context.Context, tar io.Reader, repo repository.GitRepo) error {
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	customHooksPath := filepath.Join(repoPath, customHooksDir)

	if err = os.MkdirAll(customHooksPath, os.ModePerm); err != nil {
		return fmt.Errorf("making custom hooks directory: %w", err)
	}

	lockDir, err := safe.NewLockingDirectory(repoPath, customHooksDir)
	if err != nil {
		return fmt.Errorf("creating lock: %w", err)
	}

	if err := lockDir.Lock(); err != nil {
		return fmt.Errorf("locking hooks: %w", err)
	}

	defer func() {
		if !lockDir.IsLocked() {
			return
		}

		if err := lockDir.Unlock(); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Warn("could not unlock directory")
		}
	}()

	preparedVote := voting.NewVoteHash()
	if err := voteCustomHooks(ctx, s.txManager, &preparedVote, voting.Prepared); err != nil {
		return fmt.Errorf("casting prepared vote: %w", err)
	}

	if err := extractHooks(ctx, tar, repoPath); err != nil {
		return fmt.Errorf("extracting hooks: %w", err)
	}

	committedVote, err := newDirectoryVote(customHooksPath)
	if err != nil {
		return fmt.Errorf("generating committed vote: %w", err)
	}

	if err := voteCustomHooks(ctx, s.txManager, committedVote, voting.Committed); err != nil {
		return fmt.Errorf("casting committed vote: %w", err)
	}

	if err := lockDir.Unlock(); err != nil {
		return fmt.Errorf("unlocking hooks: %w", err)
	}

	return nil
}

// newDirectoryVote creates a voting.VoteHash by walking the specified path and
// generating a hash based on file name, permissions, and data.
func newDirectoryVote(basePath string) (*voting.VoteHash, error) {
	voteHash := voting.NewVoteHash()

	if err := filepath.WalkDir(basePath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(basePath, path)
		if err != nil {
			return fmt.Errorf("getting relative path: %w", err)
		}

		// Write file relative path to hash.
		_, _ = voteHash.Write([]byte(relPath))

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("getting file info: %w", err)
		}

		// Write file permissions to hash.
		permBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(permBytes, uint32(info.Mode()))
		_, _ = voteHash.Write(permBytes)

		if entry.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file: %w", err)
		}
		defer file.Close()

		// Copy file data to hash.
		if _, err = io.Copy(voteHash, file); err != nil {
			return fmt.Errorf("copying file to hash: %w", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("walking directory: %w", err)
	}

	return &voteHash, nil
}

// voteCustomHooks casts a vote symbolic of the custom hooks received. If there
// is no transaction voting is skipped.
func voteCustomHooks(
	ctx context.Context,
	txManager transaction.Manager,
	v *voting.VoteHash,
	phase voting.Phase,
) error {
	tx, err := txinfo.TransactionFromContext(ctx)
	if errors.Is(err, txinfo.ErrTransactionNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	vote, err := v.Vote()
	if err != nil {
		return err
	}

	if err := txManager.Vote(ctx, tx, vote, phase); err != nil {
		return fmt.Errorf("vote failed: %w", err)
	}

	return nil
}

// extractHooks unpacks a tar file containing custom hooks into a `custom_hooks`
// directory at the specified path.
func extractHooks(ctx context.Context, reader io.Reader, path string) error {
	cmdArgs := []string{"-xf", "-", "-C", path, customHooksDir}

	cmd, err := command.New(ctx, append([]string{"tar"}, cmdArgs...), command.WithStdin(reader))
	if err != nil {
		return fmt.Errorf("executing tar command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("waiting for tar command completion: %w", err)
	}

	return nil
}
