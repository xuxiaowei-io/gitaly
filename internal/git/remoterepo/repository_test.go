//go:build !gitaly_test_sha256

package remoterepo_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
)

func TestRepository(t *testing.T) {
	t.Parallel()

	cfg := setupGitalyServer(t)

	pool := client.NewPool()
	defer pool.Close()

	gittest.TestRepository(t, cfg, func(tb testing.TB, ctx context.Context) (git.Repository, string) {
		tb.Helper()

		ctx, err := storage.InjectGitalyServers(ctx, "default", cfg.SocketPath, cfg.Auth.Token)
		require.NoError(tb, err)

		repoProto, repoPath := gittest.CreateRepository(tb, ctx, cfg)

		repo, err := remoterepo.New(metadata.OutgoingToIncoming(ctx), repoProto, pool)
		require.NoError(tb, err)
		return repo, repoPath
	})
}
