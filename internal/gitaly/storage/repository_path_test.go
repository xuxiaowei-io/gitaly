package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestDeriveReplicaPath(t *testing.T) {
	require.Equal(t, "@cluster/repositories/6b/86/1", storage.DeriveReplicaPath(1))
	require.Equal(t, "@cluster/repositories/d4/73/2", storage.DeriveReplicaPath(2))
}

func TestDerivePoolPath(t *testing.T) {
	require.Equal(t, "@cluster/pools/6b/86/1", storage.DerivePoolPath(1))
	require.Equal(t, "@cluster/pools/d4/73/2", storage.DerivePoolPath(2))
}

func TestIsPraefectPoolRepository(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		repo       *gitalypb.Repository
		isPoolPath bool
	}{
		{
			desc:       "missing repository",
			isPoolPath: false,
		},
		{
			desc: "empty string",
			repo: &gitalypb.Repository{
				RelativePath: "",
			},
			isPoolPath: false,
		},
		{
			desc: "praefect pool path",
			repo: &gitalypb.Repository{
				RelativePath: storage.DerivePoolPath(1),
			},
			isPoolPath: true,
		},
		{
			desc: "praefect replica path",
			repo: &gitalypb.Repository{
				RelativePath: storage.DeriveReplicaPath(1),
			},
		},
		{
			desc: "rails pool path",
			repo: &gitalypb.Repository{
				RelativePath: gittest.NewObjectPoolName(t),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.isPoolPath, storage.IsPraefectPoolRepository(tc.repo))
		})
	}
}

func TestIsPoolRepository(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		desc       string
		repo       *gitalypb.Repository
		isPoolPath bool
	}{
		{
			desc: "rails pool directory",
			repo: &gitalypb.Repository{
				RelativePath: gittest.NewObjectPoolName(t),
			},
			isPoolPath: true,
		},
		{
			desc: "praefect pool path",
			repo: &gitalypb.Repository{
				RelativePath: storage.DerivePoolPath(1),
			},
			isPoolPath: true,
		},
		{
			desc: "praefect replica path",
			repo: &gitalypb.Repository{
				RelativePath: storage.DeriveReplicaPath(1),
			},
		},
		{
			desc: "missing repository",
		},
		{
			desc: "empty repository",
			repo: &gitalypb.Repository{},
		},
		{
			desc: "rails path first to subdirs dont match full hash",
			repo: &gitalypb.Repository{
				RelativePath: "@pools/aa/bb/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.git",
			},
		},
		{
			desc: "normal repos dont match",
			repo: &gitalypb.Repository{
				RelativePath: "@hashed/" + gittest.NewRepositoryName(t),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.isPoolPath, storage.IsPoolRepository(tc.repo))
		})
	}
}
