package repository

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetConfig(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	ctx := testhelper.Context(t)

	getConfig := func(
		t *testing.T,
		client gitalypb.RepositoryServiceClient,
		repo *gitalypb.Repository,
	) (string, error) {
		stream, err := client.GetConfig(ctx, &gitalypb.GetConfigRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		reader := streamio.NewReader(func() ([]byte, error) {
			response, err := stream.Recv()
			var bytes []byte
			if response != nil {
				bytes = response.Data
			}
			return bytes, err
		})

		contents, err := io.ReadAll(reader)
		return string(contents), err
	}

	t.Run("normal repo", func(t *testing.T) {
		repo, _ := gittest.CreateRepository(t, ctx, cfg)

		config, err := getConfig(t, client, repo)
		require.NoError(t, err)

		expectedConfig := gittest.ObjectHashDependent(t, map[string]string{
			"sha1":   "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = true\n",
			"sha256": "[core]\n\trepositoryformatversion = 1\n\tfilemode = true\n\tbare = true\n[extensions]\n\tobjectformat = sha256\n",
		})

		if runtime.GOOS == "darwin" {
			expectedConfig = expectedConfig + "\tignorecase = true\n\tprecomposeunicode = true\n"
		}
		require.Equal(t, expectedConfig, config)
	})

	t.Run("missing config", func(t *testing.T) {
		repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

		configPath := filepath.Join(repoPath, "config")
		require.NoError(t, os.Remove(configPath))

		config, err := getConfig(t, client, repo)
		testhelper.RequireGrpcError(t, status.Errorf(codes.NotFound, "opening gitconfig: open %s: no such file or directory", configPath), err)
		require.Equal(t, "", config)
	})

	t.Run("no repository provided", func(t *testing.T) {
		_, err := getConfig(t, client, nil)
		testhelper.RequireGrpcError(t, testhelper.GitalyOrPraefect(
			structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
			structerr.NewInvalidArgument("repo scoped: %w", storage.ErrRepositoryNotSet),
		), err)
	})
}
