package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const attributesFileMode os.FileMode = perm.SharedFile

func (s *server) applyGitattributes(ctx context.Context, repo *localrepo.Repo, objectReader catfile.ObjectContentReader, repoPath string, revision []byte) (returnedErr error) {
	infoPath := filepath.Join(repoPath, "info")
	attributesPath := filepath.Join(infoPath, "attributes")

	_, err := repo.ResolveRevision(ctx, git.Revision(revision)+"^{commit}")
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return structerr.NewInvalidArgument("revision does not exist")
		}

		return err
	}

	blobObj, err := objectReader.Object(ctx, git.Revision(fmt.Sprintf("%s:.gitattributes", revision)))
	if err != nil && !errors.As(err, &catfile.NotFoundError{}) {
		return err
	}

	// Create  /info folder if it doesn't exist
	if err := os.MkdirAll(infoPath, perm.SharedDir); err != nil {
		return err
	}

	if errors.As(err, &catfile.NotFoundError{}) || blobObj.Type != "blob" {
		locker, err := safe.NewLockingFileWriter(attributesPath, safe.LockingFileWriterConfig{
			FileWriterConfig: safe.FileWriterConfig{FileMode: attributesFileMode},
		})
		if err != nil {
			return fmt.Errorf("creating gitattributes lock: %w", err)
		}
		defer func() {
			if err := locker.Close(); err != nil {
				s.logger.WithError(err).ErrorContext(ctx, "unlocking gitattributes")
			}
		}()

		if err := locker.Lock(); err != nil {
			return fmt.Errorf("locking gitattributes: %w", err)
		}

		// We use the zero OID as placeholder to vote on removal of the
		// gitattributes file.
		if err := s.vote(ctx, git.ObjectHashSHA1.ZeroOID, voting.Prepared); err != nil {
			return fmt.Errorf("preimage vote: %w", err)
		}

		if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
			return err
		}

		if err := s.vote(ctx, git.ObjectHashSHA1.ZeroOID, voting.Committed); err != nil {
			return fmt.Errorf("postimage vote: %w", err)
		}

		return nil
	}

	writer, err := safe.NewLockingFileWriter(attributesPath, safe.LockingFileWriterConfig{
		FileWriterConfig: safe.FileWriterConfig{FileMode: attributesFileMode},
	})
	if err != nil {
		return fmt.Errorf("creating gitattributes writer: %w", err)
	}
	defer func() {
		if err := writer.Close(); err != nil && returnedErr == nil {
			if !errors.Is(err, safe.ErrAlreadyDone) {
				returnedErr = err
			}
		}
	}()

	if _, err := io.Copy(writer, blobObj); err != nil {
		return err
	}

	if err := transaction.CommitLockedFile(ctx, s.txManager, writer); err != nil {
		return fmt.Errorf("committing gitattributes: %w", err)
	}

	return nil
}

func (s *server) vote(ctx context.Context, oid git.ObjectID, phase voting.Phase) error {
	tx, err := txinfo.TransactionFromContext(ctx)
	if errors.Is(err, txinfo.ErrTransactionNotFound) {
		return nil
	}

	hash, err := oid.Bytes()
	if err != nil {
		return fmt.Errorf("vote with invalid object ID: %w", err)
	}

	vote, err := voting.VoteFromHash(hash)
	if err != nil {
		return fmt.Errorf("cannot convert OID to vote: %w", err)
	}

	if err := s.txManager.Vote(ctx, tx, vote, phase); err != nil {
		return fmt.Errorf("vote failed: %w", err)
	}

	return nil
}

func (s *server) ApplyGitattributes(ctx context.Context, in *gitalypb.ApplyGitattributesRequest) (*gitalypb.ApplyGitattributesResponse, error) {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	repo := s.localrepo(repository)
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return nil, structerr.NewInvalidArgument("revision: %w", err)
	}

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, err
	}
	defer cancel()

	if err := s.applyGitattributes(ctx, repo, objectReader, repoPath, in.GetRevision()); err != nil {
		return nil, err
	}

	return &gitalypb.ApplyGitattributesResponse{}, nil
}
