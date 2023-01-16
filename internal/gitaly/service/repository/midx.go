package repository

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

const (
	//nolint:revive // This is unintentionally missing documentation.
	MidxRelPath = "objects/pack/multi-pack-index"
)

func (s *server) MidxRepack(ctx context.Context, in *gitalypb.MidxRepackRequest) (*gitalypb.MidxRepackResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(repository)

	if err := repo.SetConfig(ctx, "core.multiPackIndex", "true", s.txManager); err != nil {
		return nil, structerr.NewInternal("setting config: %w", err)
	}

	for _, cmd := range []midxSubCommand{s.midxWrite, s.midxExpire, s.midxRepack} {
		if err := s.safeMidxCommand(ctx, repository, cmd); err != nil {
			if git.IsInvalidArgErr(err) {
				return nil, structerr.NewInvalidArgument("%w", err)
			}

			return nil, structerr.NewInternal("...%w", err)
		}
	}

	stats.LogRepositoryInfo(ctx, repo)

	return &gitalypb.MidxRepackResponse{}, nil
}

// midxSubCommand is a helper type to group the helper functions in multi-pack-index
type midxSubCommand func(ctx context.Context, repo repository.GitRepo) error

func (s *server) safeMidxCommand(ctx context.Context, repo repository.GitRepo, cmd midxSubCommand) error {
	if err := cmd(ctx, repo); err != nil {
		return err
	}

	return s.midxEnsureExists(ctx, repo)
}

func (s *server) midxWrite(ctx context.Context, repo repository.GitRepo) error {
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.Command{
			Name:   "multi-pack-index",
			Action: "write",
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *server) midxEnsureExists(ctx context.Context, repo repository.GitRepo) error {
	ctxlogger := ctxlogrus.Extract(ctx)

	if err := s.midxVerify(ctx, repo); err != nil {
		ctxlogger.
			WithError(err).
			WithFields(log.Fields{"verify_success": false}).
			Error("MidxRepack")

		return s.midxRewrite(ctx, repo)
	}

	return nil
}

func (s *server) midxVerify(ctx context.Context, repo repository.GitRepo) error {
	ctxlogger := ctxlogrus.Extract(ctx)

	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.Command{
			Name:   "multi-pack-index",
			Action: "verify",
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}
	ctxlogger.WithFields(log.Fields{
		"verify_success": true,
	}).Debug("MidxRepack")

	return nil
}

func (s *server) midxRewrite(ctx context.Context, repo repository.GitRepo) error {
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return err
	}

	midxPath := filepath.Join(repoPath, MidxRelPath)

	if err := os.Remove(midxPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return s.midxWrite(ctx, repo)
}

func (s *server) midxExpire(ctx context.Context, repo repository.GitRepo) error {
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.Command{
			Name:   "multi-pack-index",
			Action: "expire",
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *server) midxRepack(ctx context.Context, repo repository.GitRepo) error {
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return err
	}

	batchSize, err := calculateBatchSize(repoPath)
	if err != nil {
		return err
	}

	// Do not execute a full repack with midxRepack
	// until `git multi-pack-index repack` added support
	// for bitmapindex.
	if batchSize == 0 {
		return nil
	}

	// Note that repack configs:
	//   - repack.useDeltaBaseOffset
	//   - repack.packKeptObjects
	//   - repack.useDeltaIslands
	// will only be respected if git version is >=2.28.0.
	// Bitmap index 'repack.writeBitmaps' is not yet supported.
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.Command{
			Name:   "multi-pack-index",
			Action: "repack",
			Flags: []git.Option{
				git.ValueFlag{Name: "--batch-size", Value: strconv.FormatInt(batchSize, 10)},
			},
		},
		git.WithConfig(housekeeping.GetRepackGitConfig(ctx, repo, false)...),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

// calculateBatchSize returns a batch size that is 1 greater than
// the size of the second largest packfile. This ensures that we will
// repack at least two packs if there are three or more packs.
//
// In case there are less than or equal to 2 packfiles, return 0 for
// a full repack.
//
// Reference:
// - https://public-inbox.org/git/f3b25a9927fe560b764850ea880a71932ec2af32.1598380599.git.gitgitgadget@gmail.com/
func calculateBatchSize(repoPath string) (int64, error) {
	packfiles, err := getPackfiles(repoPath)
	if err != nil {
		return 0, err
	}

	// In case of 2 or less packs,
	// batch size should be 0 for a full repack
	if len(packfiles) <= 2 {
		return 0, nil
	}

	var biggestSize int64
	var secondBiggestSize int64
	for _, packfile := range packfiles {
		info, err := packfile.Info()
		if err != nil {
			// It's fine if the entry has disappeared meanwhile, we just don't account
			// for its size.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			return 0, fmt.Errorf("statting packfile: %w", err)
		}

		if info.Size() > biggestSize {
			secondBiggestSize = biggestSize
			biggestSize = info.Size()
			continue
		}

		if info.Size() > secondBiggestSize {
			secondBiggestSize = info.Size()
		}
	}

	// Add 1 so that we always attempt to create a new
	// second biggest pack file
	return secondBiggestSize + 1, nil
}

// getPackfiles returns the FileInfo of packfiles inside a repository.
func getPackfiles(repoPath string) ([]fs.DirEntry, error) {
	files, err := os.ReadDir(filepath.Join(repoPath, "objects/pack/"))
	if err != nil {
		return nil, err
	}

	var packFiles []fs.DirEntry
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".pack" {
			packFiles = append(packFiles, f)
		}
	}

	return packFiles, nil
}
