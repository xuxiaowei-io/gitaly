package housekeeping

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func testRepoAndPool(t *testing.T, desc string, testFunc func(t *testing.T, relativePath string)) {
	t.Helper()
	t.Run(desc, func(t *testing.T) {
		t.Run("normal repository", func(t *testing.T) {
			testFunc(t, gittest.NewRepositoryName(t))
		})

		t.Run("object pool", func(t *testing.T) {
			testFunc(t, gittest.NewObjectPoolName(t))
		})
	})
}

type objectsState struct {
	looseObjects            uint64
	packfiles               uint64
	hasBitmap               bool
	hasMultiPackIndex       bool
	hasMultiPackIndexBitmap bool
}

func requireObjectsState(tb testing.TB, repo *localrepo.Repo, expectedState objectsState) {
	tb.Helper()

	repoInfo, err := stats.RepositoryInfoForRepository(repo)
	require.NoError(tb, err)

	require.Equal(tb, expectedState, objectsState{
		looseObjects:            repoInfo.LooseObjects.Count,
		packfiles:               repoInfo.Packfiles.Count,
		hasBitmap:               repoInfo.Packfiles.Bitmap.Exists,
		hasMultiPackIndex:       repoInfo.Packfiles.HasMultiPackIndex,
		hasMultiPackIndexBitmap: repoInfo.Packfiles.MultiPackIndexBitmap.Exists,
	})
}
