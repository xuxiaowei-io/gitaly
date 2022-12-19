package repository

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

const (
	mirrorRefSpec = "+refs/*:refs/*"
)

func (s *server) FetchBundle(stream gitalypb.RepositoryService_FetchBundleServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return structerr.NewInternal("first request: %w", err)
	}

	if err := service.ValidateRepository(firstRequest.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	firstRead := true
	reader := streamio.NewReader(func() ([]byte, error) {
		if firstRead {
			firstRead = false
			return firstRequest.GetData(), nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	ctx := stream.Context()
	repo := s.localrepo(firstRequest.GetRepository())
	updateHead := firstRequest.GetUpdateHead()

	tmpDir, err := tempdir.New(ctx, repo.GetStorageName(), s.locator)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	bundlePath := filepath.Join(tmpDir.Path(), "repo.bundle")
	file, err := os.Create(bundlePath)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		return structerr.NewInternal("copy bundle: %w", err)
	}

	config := []git.ConfigPair{
		{Key: "remote.inmemory.url", Value: bundlePath},
		{Key: "remote.inmemory.fetch", Value: mirrorRefSpec},
	}
	opts := localrepo.FetchOpts{
		CommandOptions: []git.CmdOpt{git.WithConfigEnv(config...)},
	}

	if err := repo.FetchRemote(ctx, "inmemory", opts); err != nil {
		return structerr.NewInternal("%w", err)
	}

	if updateHead {
		if err := s.updateHeadFromBundle(ctx, repo, bundlePath); err != nil {
			return structerr.NewInternal("%w", err)
		}
	}

	return stream.SendAndClose(&gitalypb.FetchBundleResponse{})
}

// updateHeadFromBundle updates HEAD from a bundle file
func (s *server) updateHeadFromBundle(ctx context.Context, repo *localrepo.Repo, bundlePath string) error {
	head, err := s.findBundleHead(ctx, repo, bundlePath)
	if err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}
	if head == nil {
		return nil
	}

	branch, err := repo.GuessHead(ctx, *head)
	if err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}

	if err := repo.SetDefaultBranch(ctx, s.txManager, branch); err != nil {
		return fmt.Errorf("update head from bundle: %w", err)
	}
	return nil
}

// findBundleHead tries to extract HEAD and its target from a bundle. Returns
// nil when HEAD is not found.
func (s *server) findBundleHead(ctx context.Context, repo git.RepositoryExecutor, bundlePath string) (*git.Reference, error) {
	cmd, err := repo.Exec(ctx, git.Command{
		Name:   "bundle",
		Action: "list-heads",
		Args:   []string{bundlePath, "HEAD"},
	})
	if err != nil {
		return nil, err
	}
	decoder := git.NewShowRefDecoder(cmd)
	for {
		var ref git.Reference
		err := decoder.Decode(&ref)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if ref.Name != "HEAD" {
			continue
		}
		return &ref, nil
	}
	return nil, nil
}
