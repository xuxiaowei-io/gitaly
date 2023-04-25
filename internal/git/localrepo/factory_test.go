package localrepo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestFactory(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithStorages("storage-1", "storage-2"))

	locator := config.NewLocator(cfg)
	cmdFactory, clean, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	defer clean()

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	factory := NewFactory(locator, cmdFactory, catfileCache)

	t.Run("Build", func(t *testing.T) {
		t.Run("parameters are passthrough", func(t *testing.T) {
			repo := factory.Build(&gitalypb.Repository{
				StorageName:  "non-existent",
				RelativePath: "relative-path-1",
			})

			require.Equal(t, "non-existent", repo.GitRepo.GetStorageName())
			require.Equal(t, "relative-path-1", repo.GitRepo.GetRelativePath())
		})
	})

	t.Run("ScopeByStorage/Build", func(t *testing.T) {
		t.Run("non-existent storage fails", func(t *testing.T) {
			scopedFactory, err := factory.ScopeByStorage("non-existent")
			require.ErrorContains(t, err, `no such storage: "non-existent"`)
			require.Empty(t, scopedFactory)
		})

		t.Run("successfully builds repositories", func(t *testing.T) {
			scopedFactory1, err := factory.ScopeByStorage("storage-1")
			require.NoError(t, err)

			repo1 := scopedFactory1.Build("relative-path-1")
			require.Equal(t, "storage-1", repo1.GitRepo.GetStorageName())
			require.Equal(t, "relative-path-1", repo1.GitRepo.GetRelativePath())

			scopedFactory2, err := factory.ScopeByStorage("storage-2")
			require.NoError(t, err)

			repo2 := scopedFactory2.Build("relative-path-2")
			require.Equal(t, "storage-2", repo2.GitRepo.GetStorageName())
			require.Equal(t, "relative-path-2", repo2.GitRepo.GetRelativePath())
		})
	})
}
