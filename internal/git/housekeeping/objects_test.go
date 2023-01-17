package housekeeping

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestRepackObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	// repack is a custom helper function that repacks while explicitly disabling the update of
	// server info. This is done so that we assert that the actual repacking logic doesn't write
	// the server info.
	repack := func(t *testing.T, repoPath string, args ...string) {
		gittest.Exec(t, cfg, append([]string{
			"-c", "repack.updateServerInfo=false", "-C", repoPath, "repack",
		}, args...)...)
	}

	for _, tc := range []struct {
		desc              string
		setup             func(t *testing.T, repoPath string)
		repackCfg         RepackObjectsConfig
		stateBeforeRepack objectsState
		stateAfterRepack  objectsState
		expectedErr       error
	}{
		{
			desc: "incremental repack packs objects",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			},
			stateBeforeRepack: objectsState{
				looseObjects: 2,
			},
			stateAfterRepack: objectsState{
				packfiles: 1,
			},
		},
		{
			desc: "incremental repack does not repack packfiles",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("first"))
				repack(t, repoPath, "-d")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("second"), gittest.WithBranch("second"))
				repack(t, repoPath, "-d")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("third"), gittest.WithBranch("third"))
			},
			stateBeforeRepack: objectsState{
				packfiles:    2,
				looseObjects: 1,
			},
			stateAfterRepack: objectsState{
				packfiles: 3,
			},
		},
		{
			desc: "incremental repack with bitmap returns an error",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("first"))
			},
			repackCfg: RepackObjectsConfig{
				WriteBitmap: true,
			},
			stateBeforeRepack: objectsState{
				looseObjects: 2,
			},
			stateAfterRepack: objectsState{
				looseObjects: 2,
			},
			expectedErr: structerr.NewInvalidArgument("cannot write bitmap for an incremental repack"),
		},
		{
			desc: "full repack packs packfiles and loose objects",
			setup: func(t *testing.T, repoPath string) {
				// We seed the repository so that it contains two packfiles and one loose object.
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("first"))
				repack(t, repoPath, "-d")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("second"), gittest.WithBranch("second"))
				repack(t, repoPath, "-d")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("third"), gittest.WithBranch("third"))
			},
			repackCfg: RepackObjectsConfig{
				FullRepack: true,
			},
			stateBeforeRepack: objectsState{
				looseObjects: 1,
				packfiles:    2,
			},
			stateAfterRepack: objectsState{
				packfiles: 1,
			},
		},
		{
			desc: "full repack packs packfiles and loose objects and writes a bitmap",
			setup: func(t *testing.T, repoPath string) {
				// We seed the repository so that it contains two packfiles and one loose object.
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("first"))
				repack(t, repoPath, "-d")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("second"), gittest.WithBranch("second"))
				repack(t, repoPath, "-d")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("third"), gittest.WithBranch("third"))
			},
			repackCfg: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			},
			stateBeforeRepack: objectsState{
				looseObjects: 1,
				packfiles:    2,
			},
			stateAfterRepack: objectsState{
				packfiles: 1,
				hasBitmap: true,
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.setup(t, repoPath)

			requireObjectsState(t, repo, tc.stateBeforeRepack)
			require.Equal(t, tc.expectedErr, RepackObjects(ctx, repo, tc.repackCfg))
			requireObjectsState(t, repo, tc.stateAfterRepack)

			// There should not be any server info data in the repository.
			require.NoFileExists(t, filepath.Join(repoPath, "info", "refs"))
			require.NoFileExists(t, filepath.Join(repoPath, "objects", "info", "packs"))
		})
	}

	testRepoAndPool(t, "delta islands", func(t *testing.T, relativePath string) {
		t.Parallel()

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           relativePath,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		gittest.TestDeltaIslands(t, cfg, repoPath, repoPath, IsPoolRepository(repoProto), func() error {
			return RepackObjects(ctx, repo, RepackObjectsConfig{
				FullRepack: true,
			})
		})
	})
}
