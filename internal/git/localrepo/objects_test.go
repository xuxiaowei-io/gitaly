package localrepo

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"google.golang.org/grpc/metadata"
)

func TestRepo_ReadObject(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath := setupRepo(t)
	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("content"))

	for _, tc := range []struct {
		desc    string
		oid     git.ObjectID
		content string
		error   error
	}{
		{
			desc:  "invalid object",
			oid:   gittest.DefaultObjectHash.ZeroOID,
			error: InvalidObjectError(gittest.DefaultObjectHash.ZeroOID.String()),
		},
		{
			desc:    "valid object",
			oid:     blobID,
			content: "content",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			content, err := repo.ReadObject(ctx, tc.oid)
			require.Equal(t, tc.error, err)
			require.Equal(t, tc.content, string(content))
		})
	}
}

func TestRepoReadObjectInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath := setupRepo(t)
	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("content"))
	objectHash, err := repo.ObjectHash(ctx)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc               string
		oid                git.ObjectID
		content            string
		expectedErr        error
		expectedObjectInfo catfile.ObjectInfo
	}{
		{
			desc:        "missing object",
			oid:         git.ObjectID("abcdefg"),
			expectedErr: InvalidObjectError("abcdefg"),
		},
		{
			desc:    "valid object",
			oid:     blobID,
			content: "content",
			expectedObjectInfo: catfile.ObjectInfo{
				Oid:    blobID,
				Type:   "blob",
				Size:   7,
				Format: objectHash.Format,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			info, err := repo.ReadObjectInfo(ctx, git.Revision(tc.oid))
			require.Equal(t, tc.expectedErr, err)
			if tc.expectedErr == nil {
				require.Equal(t, tc.expectedObjectInfo, *info)
			}
		})
	}
}

func TestRepo_ReadObject_catfileCount(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	gitCmdFactory := gittest.NewCountingCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	// Session needs to be set for the catfile cache to operate
	ctx = testhelper.MergeIncomingMetadata(ctx,
		metadata.Pairs(catfile.SessionIDField, "1"),
	)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := New(testhelper.NewLogger(t), config.NewLocator(cfg), gitCmdFactory, catfileCache, repoProto)

	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("content"))

	for i := 0; i < 10; i++ {
		content, err := repo.ReadObject(ctx, blobID)
		require.NoError(t, err)
		require.Equal(t, "content", string(content))
	}

	gitCmdFactory.RequireCommandCount(t, "cat-file", 1)
}

func TestWalkUnreachableObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath := setupRepo(t)

	commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("commit-1"))
	unreachableCommit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commit1))
	unreachableCommit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(unreachableCommit1))
	prunedCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(unreachableCommit2))
	brokenParent1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(prunedCommit))

	// Pack brokenParent so we can unpack it into the repository as an object with broken links after
	// pruning.
	var packedBrokenParent bytes.Buffer
	require.NoError(t, repo.PackObjects(ctx, strings.NewReader(brokenParent1.String()), &packedBrokenParent))

	// Prune to remove the prunedCommit.
	gittest.Exec(t, cfg, "-C", repoPath, "prune", unreachableCommit1.String(), unreachableCommit2.String())

	// Unpack brokenParent now that the parent has been pruned.
	require.NoError(t, repo.UnpackObjects(ctx, &packedBrokenParent))

	require.ElementsMatch(t,
		[]git.ObjectID{
			gittest.DefaultObjectHash.EmptyTreeOID,
			commit1,
			unreachableCommit1,
			unreachableCommit2,
			brokenParent1,
		},
		gittest.ListObjects(t, cfg, repoPath),
	)

	for _, tc := range []struct {
		desc           string
		heads          []git.ObjectID
		expectedOutput []string
		expectedError  error
	}{
		{
			desc: "no heads",
		},
		{
			desc:  "reachable commit not reported",
			heads: []git.ObjectID{commit1},
		},
		{
			desc:  "unreachable commits reported",
			heads: []git.ObjectID{unreachableCommit2},
			expectedOutput: []string{
				unreachableCommit1.String(),
				unreachableCommit2.String(),
			},
		},
		{
			desc:          "non-existent head",
			heads:         []git.ObjectID{prunedCommit},
			expectedError: BadObjectError{ObjectID: prunedCommit},
		},
		{
			desc:          "traversal fails due to missing parent commit",
			heads:         []git.ObjectID{brokenParent1},
			expectedError: ObjectReadError{prunedCommit},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			var heads []string
			for _, head := range tc.heads {
				heads = append(heads, head.String())
			}

			var output bytes.Buffer
			require.Equal(t,
				tc.expectedError,
				repo.WalkUnreachableObjects(ctx, strings.NewReader(strings.Join(heads, "\n")), &output))

			var actualOutput []string
			if output.Len() > 0 {
				actualOutput = strings.Split(strings.TrimSpace(output.String()), "\n")
			}
			require.ElementsMatch(t, tc.expectedOutput, actualOutput)
		})
	}
}

func TestPackAndUnpackObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, repoPath := setupRepo(t)

	commit1 := gittest.WriteCommit(t, cfg, repoPath)
	commit2 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commit1))
	commit3 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commit2))

	require.ElementsMatch(t,
		[]git.ObjectID{
			gittest.DefaultObjectHash.EmptyTreeOID,
			commit1,
			commit2,
			commit3,
		},
		gittest.ListObjects(t, cfg, repoPath),
	)

	var emptyPack bytes.Buffer
	require.NoError(t,
		repo.PackObjects(ctx, strings.NewReader(""),
			&emptyPack,
		),
	)

	var oneCommitPack bytes.Buffer
	require.NoError(t,
		repo.PackObjects(ctx, strings.NewReader(
			strings.Join([]string{commit1.String()}, "\n"),
		),
			&oneCommitPack,
		),
	)

	var twoCommitPack bytes.Buffer
	require.NoError(t,
		repo.PackObjects(ctx, strings.NewReader(
			strings.Join([]string{commit1.String(), commit2.String()}, "\n"),
		),
			&twoCommitPack,
		),
	)

	var incompletePack bytes.Buffer
	require.NoError(t,
		repo.PackObjects(ctx, strings.NewReader(
			strings.Join([]string{commit1.String(), commit3.String()}, "\n"),
		),
			&incompletePack,
		),
	)

	for _, tc := range []struct {
		desc                 string
		pack                 []byte
		expectedObjects      []git.ObjectID
		expectedErrorMessage string
	}{
		{
			desc: "empty pack",
			pack: emptyPack.Bytes(),
		},
		{
			desc: "one commit",
			pack: oneCommitPack.Bytes(),
			expectedObjects: []git.ObjectID{
				commit1,
			},
		},
		{
			desc: "two commits",
			pack: twoCommitPack.Bytes(),
			expectedObjects: []git.ObjectID{
				commit1, commit2,
			},
		},
		{
			desc: "incomplete pack",
			pack: incompletePack.Bytes(),
			expectedObjects: []git.ObjectID{
				commit1, commit3,
			},
		},
		{
			desc:                 "no pack",
			expectedErrorMessage: "unpack objects: exit status 128",
		},
		{
			desc:                 "broken pack",
			pack:                 []byte("invalid pack"),
			expectedErrorMessage: "unpack objects: exit status 128",
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg, repo, repoPath := setupRepo(t)
			require.Empty(t, gittest.ListObjects(t, cfg, repoPath))

			err := repo.UnpackObjects(ctx, bytes.NewReader(tc.pack))
			if tc.expectedErrorMessage != "" {
				require.EqualError(t, err, tc.expectedErrorMessage)
			} else {
				require.NoError(t, err)
			}
			require.ElementsMatch(t, tc.expectedObjects, gittest.ListObjects(t, cfg, repoPath))
		})
	}
}
