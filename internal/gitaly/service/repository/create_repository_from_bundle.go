package repository

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

func (s *server) CreateRepositoryFromBundle(stream gitalypb.RepositoryService_CreateRepositoryFromBundleServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return structerr.NewInternal("first request failed: %w", err)
	}

	repository := firstRequest.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("CreateRepositoryFromBundle: %w", err)
	}

	ctx := stream.Context()

	firstRead := false
	bundleReader := streamio.NewReader(func() ([]byte, error) {
		if !firstRead {
			firstRead = true
			return firstRequest.GetData(), nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	repo := repository
	bundleDir, err := tempdir.New(ctx, repo.GetStorageName(), s.locator)
	if err != nil {
		return structerr.NewInternal("creating bundle directory: %w", err)
	}

	bundlePath := filepath.Join(bundleDir.Path(), "repo.bundle")
	bundleFile, err := os.Create(bundlePath)
	if err != nil {
		return structerr.NewInternal("creating bundle file: %w", err)
	}

	if _, err := io.Copy(bundleFile, bundleReader); err != nil {
		return structerr.NewInternal("writing bundle file: %w", err)
	}

	if err := repoutil.Create(ctx, s.locator, s.gitCmdFactory, s.txManager, repo, func(repo *gitalypb.Repository) error {
		var stderr bytes.Buffer
		cmd, err := s.gitCmdFactory.New(ctx, repo, git.Command{
			Name: "fetch",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--atomic"},
			},
			Args: []string{bundlePath, "refs/*:refs/*"},
		}, git.WithStderr(&stderr), git.WithRefTxHook(repo))
		if err != nil {
			return structerr.NewInternal("spawning fetch: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			sanitizedStderr := sanitizedError("%s", stderr.String())
			return structerr.NewInternal("fetch from bundle: %w, stderr: %q", err, sanitizedStderr)
		}

		if err := s.updateHeadFromBundle(ctx, s.localrepo(repo), bundlePath); err != nil {
			return structerr.NewInternal("%w", err)
		}

		return nil
	}); err != nil {
		return structerr.NewInternal("creating repository: %w", err)
	}

	return stream.SendAndClose(&gitalypb.CreateRepositoryFromBundleResponse{})
}

func sanitizedError(path, format string, a ...interface{}) string {
	str := fmt.Sprintf(format, a...)
	return strings.Replace(str, path, "[REPO PATH]", -1)
}
