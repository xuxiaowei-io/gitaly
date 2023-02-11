package repository

import (
	"context"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v15/structerr"
	"os"
	"path/filepath"
	"regexp"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

var objectFiles = []*regexp.Regexp{
	regexp.MustCompile(`/[[:xdigit:]]{2}/[[:xdigit:]]{38}\z`),
	regexp.MustCompile(`/pack/pack\-[[:xdigit:]]{40}\.(pack|idx)\z`),
}

func (s *server) GetSnapshot(in *gitalypb.GetSnapshotRequest, stream gitalypb.RepositoryService_GetSnapshotServer) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	path, err := s.locator.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetSnapshotResponse{Data: p})
	})

	// Building a raw archive may race with `git push`, but GitLab can enforce
	// concurrency control if necessary. Using `TarBuilder` means we can keep
	// going even if some files are added or removed during the operation.
	builder := archive.NewTarBuilder(path, writer)

	// Pick files directly by filename so we can get a snapshot even if the
	// repository is corrupted. https://gitirc.eu/gitrepository-layout.html
	// documents the various files and directories. We exclude the following
	// on purpose:
	//
	//   * branches - legacy, not replicated by git fetch
	//   * commondir - may differ between sites
	//   * config - may contain credentials, and cannot be managed by client
	//   * custom-hooks - GitLab-specific, no supported in Geo, may differ between sites
	//   * hooks - symlink, may differ between sites
	//   * {shared,}index[.*] - not found in bare repositories
	//   * info/{attributes,exclude,grafts} - not replicated by git fetch
	//   * info/refs - dumb protocol only
	//   * logs/* - not replicated by git fetch
	//   * modules/* - not replicated by git fetch
	//   * objects/info/* - unneeded (dumb protocol) or to do with alternates
	//   * worktrees/* - not replicated by git fetch

	// References
	_ = builder.FileIfExist("HEAD")
	_ = builder.FileIfExist("packed-refs")
	_ = builder.RecursiveDirIfExist("refs")
	_ = builder.RecursiveDirIfExist("branches")

	// The packfiles + any loose objects.
	_ = builder.RecursiveDirIfExist("objects", objectFiles...)

	// In case this repository is a shallow clone. Seems unlikely, but better
	// safe than sorry.
	_ = builder.FileIfExist("shallow")

	if err := s.addAlternateFiles(stream.Context(), in.GetRepository(), builder); err != nil {
		return structerr.NewInternal("add alternates: %w", err)
	}

	if err := builder.Close(); err != nil {
		return structerr.NewInternal("building snapshot failed: %w", err)
	}

	return nil
}

func (s *server) addAlternateFiles(ctx context.Context, repository *gitalypb.Repository, builder *archive.TarBuilder) error {
	storageRoot, err := s.locator.GetStorageByName(repository.GetStorageName())
	if err != nil {
		return fmt.Errorf("get storage path: %w", err)
	}

	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	altObjDirs, err := git.AlternateObjectDirectories(ctx, storageRoot, repoPath)
	if err != nil {
		ctxlogrus.Extract(ctx).WithField("error", err).Warn("error getting alternate object directories")
		return nil
	}

	for _, altObjDir := range altObjDirs {
		if err := walkAndAddToBuilder(altObjDir, builder); err != nil {
			return fmt.Errorf("walking alternates file: %w", err)
		}
	}

	return nil
}

func walkAndAddToBuilder(alternateObjDir string, builder *archive.TarBuilder) error {
	matchWalker := archive.NewMatchWalker(objectFiles, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error walking %v: %w", path, err)
		}

		relPath, err := filepath.Rel(alternateObjDir, path)
		if err != nil {
			return fmt.Errorf("alternative object directory path: %w", err)
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", path, err)
		}
		defer file.Close()

		objectPath := filepath.Join("objects", relPath)

		if err := builder.VirtualFileWithContents(objectPath, file); err != nil {
			return fmt.Errorf("expected file %v to exist: %w", path, err)
		}

		return nil
	})

	if err := filepath.Walk(alternateObjDir, matchWalker.Walk); err != nil {
		return fmt.Errorf("error when traversing: %w", err)
	}

	return nil
}
