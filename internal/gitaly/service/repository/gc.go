package repository

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) GarbageCollect(ctx context.Context, in *gitalypb.GarbageCollectRequest) (*gitalypb.GarbageCollectResponse, error) {
	ctxlogger := ctxlogrus.Extract(ctx)
	ctxlogger.WithFields(log.Fields{
		"WriteBitmaps": in.GetCreateBitmap(),
	}).Debug("GarbageCollect")

	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(repository)

	if err := housekeeping.CleanupWorktrees(ctx, repo); err != nil {
		return nil, err
	}

	if err := s.cleanupKeepArounds(ctx, repo); err != nil {
		return nil, err
	}

	// Perform housekeeping to cleanup stale lockfiles that may block GC
	if err := s.housekeepingManager.CleanStaleData(ctx, repo); err != nil {
		ctxlogger.WithError(err).Warn("Pre gc housekeeping failed")
	}

	if err := s.gc(ctx, in); err != nil {
		return nil, err
	}

	if err := housekeeping.WriteCommitGraph(ctx, repo, housekeeping.WriteCommitGraphConfig{
		ReplaceChain: true,
	}); err != nil {
		return nil, err
	}

	stats.LogRepositoryInfo(ctx, repo)

	return &gitalypb.GarbageCollectResponse{}, nil
}

func (s *server) gc(ctx context.Context, in *gitalypb.GarbageCollectRequest) error {
	config := append(housekeeping.GetRepackGitConfig(ctx, in.GetRepository(), in.CreateBitmap), git.ConfigPair{Key: "gc.writeCommitGraph", Value: "false"})

	var flags []git.Option
	if in.Prune {
		flags = append(flags, git.Flag{Name: "--prune=30.minutes.ago"})
	}

	cmd, err := s.gitCmdFactory.New(ctx, in.GetRepository(),
		git.Command{Name: "gc", Flags: flags},
		git.WithConfig(config...),
	)
	if err != nil {
		if git.IsInvalidArgErr(err) {
			return structerr.NewInvalidArgument("gitCommand: %w", err)
		}

		return structerr.NewInternal("gitCommand: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return structerr.NewInternal("cmd wait: %w", err)
	}

	return nil
}

func (s *server) cleanupKeepArounds(ctx context.Context, repo *localrepo.Repo) error {
	repoPath, err := repo.Path()
	if err != nil {
		return nil
	}

	objectInfoReader, cancel, err := s.catfileCache.ObjectInfoReader(ctx, repo)
	if err != nil {
		return nil
	}
	defer cancel()

	keepAroundsPrefix := "refs/keep-around"
	keepAroundsPath := filepath.Join(repoPath, keepAroundsPrefix)

	refEntries, err := os.ReadDir(keepAroundsPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, entry := range refEntries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			// Even though the reference has disappeared we know its name
			// and thus the object hash it is supposed to point to. So
			// while we technically could try to fix it, we don't as
			// we seem to be racing with a concurrent process.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			return fmt.Errorf("statting keep-around ref: %w", err)
		}

		refName := fmt.Sprintf("%s/%s", keepAroundsPrefix, info.Name())
		path := filepath.Join(repoPath, keepAroundsPrefix, info.Name())

		if err = checkRef(ctx, objectInfoReader, refName, info); err == nil {
			continue
		}

		if err := s.fixRef(ctx, repo, objectInfoReader, path, refName, info.Name()); err != nil {
			return err
		}
	}

	return nil
}

func checkRef(ctx context.Context, objectInfoReader catfile.ObjectInfoReader, refName string, info os.FileInfo) error {
	if info.Size() == 0 {
		return errors.New("checkRef: Ref file is empty")
	}

	_, err := objectInfoReader.Info(ctx, git.Revision(refName))
	return err
}

func (s *server) fixRef(ctx context.Context, repo *localrepo.Repo, objectInfoReader catfile.ObjectInfoReader, refPath string, name string, sha string) error {
	// So the ref is broken, let's get rid of it
	if err := os.RemoveAll(refPath); err != nil {
		return err
	}

	// If the sha is not in the the repository, we can't fix it
	if _, err := objectInfoReader.Info(ctx, git.Revision(sha)); err != nil {
		return nil
	}

	// The name is a valid sha, recreate the ref
	return repo.ExecAndWait(ctx, git.Command{
		Name: "update-ref",
		Args: []string{name, sha},
	}, git.WithRefTxHook(repo))
}
