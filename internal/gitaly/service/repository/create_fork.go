package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) CreateFork(ctx context.Context, req *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error) {
	if err := service.ValidateRepository(req.GetSourceRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("validating source repository: %w", err)
	}
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	targetRepository := req.Repository
	sourceRepository := req.SourceRepository

	var objectPoolPath string
	if req.GetMode() != nil {
		switch req.Mode.(type) {
		case *gitalypb.CreateForkRequest_LinkToObjectPool:
			objectPoolProto := req.GetLinkToObjectPool().GetObjectPool()

			if err := service.ValidateRepository(objectPoolProto.GetRepository()); err != nil {
				return nil, structerr.NewInvalidArgument("validating object pool: %w", err)
			}
			if objectPoolProto.GetRepository().GetStorageName() != targetRepository.StorageName {
				return nil, structerr.NewInvalidArgument("cannot link to object pool on different storage")
			}

			objectPool, err := objectpool.FromProto(
				s.locator,
				s.gitCmdFactory,
				s.catfileCache,
				s.txManager,
				s.housekeepingManager,
				objectPoolProto,
			)
			if err != nil {
				if errors.Is(err, objectpool.ErrAlternateObjectDirNotExist) {
					return nil, structerr.NewFailedPrecondition("cannot link against repository without object pool")
				}
				if errors.Is(err, objectpool.ErrInvalidPoolRepository) {
					return nil, structerr.NewFailedPrecondition("object pool of source repository is not a valid repository")
				}

				return nil, fmt.Errorf("retrieving object pool: %w", err)
			}

			objectPoolPath, err = objectPool.Path()
			if err != nil {
				return nil, fmt.Errorf("getting object pool path: %w", err)
			}
		default:
			return nil, structerr.NewInvalidArgument("unsupported mode: %T", req.Mode)
		}
	}

	if err := repoutil.Create(ctx, s.locator, s.gitCmdFactory, s.txManager, targetRepository, func(repo *gitalypb.Repository) error {
		targetPath, err := s.locator.GetPath(repo)
		if err != nil {
			return err
		}

		options := []git.Option{
			git.Flag{Name: "--bare"},
			git.Flag{Name: "--quiet"},
		}

		if objectPoolPath != "" {
			options = append(options, git.ValueFlag{Name: "--reference", Value: objectPoolPath})
		}

		// Ideally we'd just fetch into the already-created repo, but that wouldn't
		// allow us to easily set up HEAD to point to the correct ref. We thus have
		// no easy choice but to use git-clone(1).
		var stderr strings.Builder
		cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
			git.Command{
				Name:  "clone",
				Flags: options,
				Args: []string{
					git.InternalGitalyURL,
					targetPath,
				},
			},
			git.WithInternalFetch(&gitalypb.SSHUploadPackRequest{
				Repository: sourceRepository,
			}),
			git.WithConfig(git.ConfigPair{
				// Disable consistency checks for fetched objects when creating a
				// fork. We don't want to end up in a situation where it's
				// impossible to create forks we already have anyway because we have
				// e.g. retroactively tightened the consistency checks.
				Key: "fetch.fsckObjects", Value: "false",
			}),
			git.WithDisabledHooks(),
			git.WithStderr(&stderr),
		)
		if err != nil {
			return fmt.Errorf("spawning fetch: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("fetching source repo: %w, stderr: %q", err, stderr.String())
		}

		if err := s.removeOriginInRepo(ctx, repo); err != nil {
			return fmt.Errorf("removing origin remote: %w", err)
		}

		// The object pool path written by Git will be an absolute path. Because we want to
		// support that repositories can be moved to a different directory hierarchy we
		// rewrite these absolute paths to be relative paths instead.
		if objectPoolPath != "" {
			finalPath, err := s.locator.GetRepoPath(targetRepository, storage.WithRepositoryVerificationSkipped())
			if err != nil {
				return fmt.Errorf("computing final path: %w", err)
			}

			// We compute the relative path against the final path of where the
			// repository will be written to.
			relativeObjectPoolPath, err := filepath.Rel(
				filepath.Join(finalPath, "objects"),
				filepath.Join(objectPoolPath, "objects"),
			)
			if err != nil {
				return fmt.Errorf("computing relative object pool path: %w", err)
			}

			// But we write it into the staged repository that currently still lives in
			// a temporary directory.
			if err := os.WriteFile(
				filepath.Join(targetPath, "objects", "info", "alternates"),
				[]byte(relativeObjectPoolPath),
				perm.PublicFile,
			); err != nil {
				return fmt.Errorf("rewriting object pool path: %w", err)
			}
		}

		return nil
	}, repoutil.WithSkipInit()); err != nil {
		return nil, structerr.NewInternal("creating fork: %w", err)
	}

	return &gitalypb.CreateForkResponse{}, nil
}
