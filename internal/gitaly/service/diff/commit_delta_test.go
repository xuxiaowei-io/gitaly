//go:build !gitaly_test_sha256

package diff

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitDeltaRequest(t *testing.T) {
	ctx := testhelper.Context(t)
	_, repo, _, client := setupDiffService(t, ctx)

	rightCommit := "742518b2be68fc750bb4c357c0df821a88113286"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.CommitDeltaRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit}
	c, err := client.CommitDelta(ctx, rpcRequest)
	require.NoError(t, err)

	expectedDeltas := []diff.Diff{
		{
			FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
			ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
		},
		{
			FromID:   "bdea48ee65c869eb0b86b1283069d76cce0a7254",
			ToID:     git.ObjectHashSHA1.ZeroOID.String(),
			OldMode:  0o100644,
			NewMode:  0,
			FromPath: []byte("gitaly/deleted-file"),
			ToPath:   []byte("gitaly/deleted-file"),
		},
		{
			FromID:   "aa408b4556e594f7974390ad6b86210617fbda6e",
			ToID:     "1c69c4d2a65ad05c24ac3b6780b5748b97ffd3aa",
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("gitaly/file-with-multiple-chunks"),
			ToPath:   []byte("gitaly/file-with-multiple-chunks"),
		},
		{
			FromID:   git.ObjectHashSHA1.ZeroOID.String(),
			ToID:     "bc2ef601a538d69ef99d5bdafa605e63f902e8e4",
			OldMode:  0,
			NewMode:  0o100644,
			FromPath: []byte("gitaly/logo-white.png"),
			ToPath:   []byte("gitaly/logo-white.png"),
		},
		{
			FromID:   "ead5a0eee1391308803cfebd8a2a8530495645eb",
			ToID:     "ead5a0eee1391308803cfebd8a2a8530495645eb",
			OldMode:  0o100644,
			NewMode:  0o100755,
			FromPath: []byte("gitaly/mode-file"),
			ToPath:   []byte("gitaly/mode-file"),
		},
		{
			FromID:   "357406f3075a57708d0163752905cc1576fceacc",
			ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
			OldMode:  0o100644,
			NewMode:  0o100755,
			FromPath: []byte("gitaly/mode-file-with-mods"),
			ToPath:   []byte("gitaly/mode-file-with-mods"),
		},
		{
			FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
			ToID:     git.ObjectHashSHA1.ZeroOID.String(),
			OldMode:  0o100644,
			NewMode:  0,
			FromPath: []byte("gitaly/named-file-with-mods"),
			ToPath:   []byte("gitaly/named-file-with-mods"),
		},
		{
			FromID:   git.ObjectHashSHA1.ZeroOID.String(),
			ToID:     "b464dff7a75ccc92fbd920fd9ae66a84b9d2bf94",
			OldMode:  0,
			NewMode:  0o100644,
			FromPath: []byte("gitaly/no-newline-at-the-end"),
			ToPath:   []byte("gitaly/no-newline-at-the-end"),
		},
		{
			FromID:   "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
			ToID:     "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("gitaly/named-file"),
			ToPath:   []byte("gitaly/renamed-file"),
		},
		{
			FromID:   git.ObjectHashSHA1.ZeroOID.String(),
			ToID:     "3856c00e9450a51a62096327167fc43d3be62eef",
			OldMode:  0,
			NewMode:  0o100644,
			FromPath: []byte("gitaly/renamed-file-with-mods"),
			ToPath:   []byte("gitaly/renamed-file-with-mods"),
		},
		{
			FromID:   git.ObjectHashSHA1.ZeroOID.String(),
			ToID:     "a135e3e0d4af177a902ca57dcc4c7fc6f30858b1",
			OldMode:  0,
			NewMode:  0o100644,
			FromPath: []byte("gitaly/tab\tnewline\n file"),
			ToPath:   []byte("gitaly/tab\tnewline\n file"),
		},
		{
			FromID:   git.ObjectHashSHA1.ZeroOID.String(),
			ToID:     "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
			OldMode:  0,
			NewMode:  0o100755,
			FromPath: []byte("gitaly/テスト.txt"),
			ToPath:   []byte("gitaly/テスト.txt"),
		},
	}

	assertExactReceivedDeltas(t, c, expectedDeltas)
}

func TestSuccessfulCommitDeltaRequestWithPaths(t *testing.T) {
	ctx := testhelper.Context(t)
	_, repo, _, client := setupDiffService(t, ctx)

	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.CommitDeltaRequest{
		Repository:    repo,
		RightCommitId: rightCommit,
		LeftCommitId:  leftCommit,
		Paths: [][]byte{
			[]byte("CONTRIBUTING.md"),
			[]byte("README.md"),
			[]byte("gitaly/named-file-with-mods"),
			[]byte("gitaly/mode-file-with-mods"),
		},
	}
	c, err := client.CommitDelta(ctx, rpcRequest)
	require.NoError(t, err)

	expectedDeltas := []diff.Diff{
		{
			FromID:   "c1788657b95998a2f177a4f86d68a60f2a80117f",
			ToID:     "b87f61fe2d7b2e208b340a1f3cafea916bd27f75",
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("CONTRIBUTING.md"),
			ToPath:   []byte("CONTRIBUTING.md"),
		},
		{
			FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
			ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
			OldMode:  0o100644,
			NewMode:  0o100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
		},
		{
			FromID:   "357406f3075a57708d0163752905cc1576fceacc",
			ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
			OldMode:  0o100644,
			NewMode:  0o100755,
			FromPath: []byte("gitaly/mode-file-with-mods"),
			ToPath:   []byte("gitaly/mode-file-with-mods"),
		},
		{
			FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
			ToID:     git.ObjectHashSHA1.ZeroOID.String(),
			OldMode:  0o100644,
			NewMode:  0,
			FromPath: []byte("gitaly/named-file-with-mods"),
			ToPath:   []byte("gitaly/named-file-with-mods"),
		},
	}

	assertExactReceivedDeltas(t, c, expectedDeltas)
}

func TestFailedCommitDeltaRequestDueToValidationError(t *testing.T) {
	ctx := testhelper.Context(t)
	_, repo, _, client := setupDiffService(t, ctx)

	rightCommit := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"
	leftCommit := rightCommit + "~" // Parent of rightCommit

	rpcRequests := []*gitalypb.CommitDeltaRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, RightCommitId: rightCommit, LeftCommitId: leftCommit}, // Repository doesn't exist
		{Repository: nil, RightCommitId: rightCommit, LeftCommitId: leftCommit},                                                             // Repository is nil
		{Repository: repo, RightCommitId: "", LeftCommitId: leftCommit},                                                                     // RightCommitId is empty
		{Repository: repo, RightCommitId: rightCommit, LeftCommitId: ""},                                                                    // LeftCommitId is empty
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			c, err := client.CommitDelta(ctx, rpcRequest)
			require.NoError(t, err)

			err = drainCommitDeltaResponse(c)
			testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestFailedCommitDeltaRequestWithNonExistentCommit(t *testing.T) {
	ctx := testhelper.Context(t)
	_, repo, _, client := setupDiffService(t, ctx)

	nonExistentCommitID := "deadfacedeadfacedeadfacedeadfacedeadface"
	leftCommit := nonExistentCommitID + "~" // Parent of rightCommit
	rpcRequest := &gitalypb.CommitDeltaRequest{Repository: repo, RightCommitId: nonExistentCommitID, LeftCommitId: leftCommit}
	c, err := client.CommitDelta(ctx, rpcRequest)
	require.NoError(t, err)

	err = drainCommitDeltaResponse(c)
	testhelper.RequireGrpcCode(t, err, codes.FailedPrecondition)
}

func drainCommitDeltaResponse(c gitalypb.DiffService_CommitDeltaClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func assertExactReceivedDeltas(t *testing.T, client gitalypb.DiffService_CommitDeltaClient, expectedDeltas []diff.Diff) {
	t.Helper()

	counter := 0
	for {
		fetchedDeltas, err := client.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for _, fetchedDelta := range fetchedDeltas.GetDeltas() {
			require.GreaterOrEqual(t, len(expectedDeltas), counter, "Unexpected delta #%d received: %v", counter, fetchedDelta)

			expectedDelta := expectedDeltas[counter]

			require.Equal(t, expectedDelta.FromID, fetchedDelta.FromId)
			require.Equal(t, expectedDelta.ToID, fetchedDelta.ToId)
			require.Equal(t, expectedDelta.OldMode, fetchedDelta.OldMode)
			require.Equal(t, expectedDelta.NewMode, fetchedDelta.NewMode)
			require.Equal(t, expectedDelta.FromPath, fetchedDelta.FromPath)
			require.Equal(t, expectedDelta.ToPath, fetchedDelta.ToPath)

			counter++
		}
	}

	require.Len(t, expectedDeltas, counter, "Unexpected number of deltas")
}
