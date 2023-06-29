//go:build !gitaly_test_sha256

package commit

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulGetCommitSignaturesRequest(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(t, testSuccessfulGetCommitSignaturesRequest)
}

func testSuccessfulGetCommitSignaturesRequest(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	cfg.Git.SigningKey = "testdata/signing_ssh_key_ed25519"
	cfg.SocketPath = startTestServices(t, cfg)
	client := newCommitServiceClient(t, cfg.SocketPath)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	commitData := testhelper.MustReadFile(t, "testdata/dc00eb001f41dfac08192ead79c2377c588b82ee.commit")
	commit := text.ChompBytes(gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewReader(commitData)},
		"-C", repoPath, "hash-object", "-w", "-t", "commit", "--stdin", "--literally",
	))
	require.Equal(t, "dc00eb001f41dfac08192ead79c2377c588b82ee", commit)

	signedByGitalyOid := commitSignedByGitaly(t, ctx, repo, cfg)
	request := &gitalypb.GetCommitSignaturesRequest{
		Repository: repo,
		CommitIds: []string{
			"5937ac0a7beb003549fc5fd26fc247adbce4a52e", // has signature
			"e63f41fe459e62e1228fcef60d7189127aeba95a", // has no signature
			git.ObjectHashSHA1.ZeroOID.String(),        // does not exist
			"a17a9f66543673edf0a3d1c6b93bdda3fe600f32", // has signature
			"8cf8e80a5a0546e391823c250f2b26b9cf15ce88", // has signature and commit message > 4MB
			"dc00eb001f41dfac08192ead79c2377c588b82ee", // has signature and commit message without newline at the end
			"7b5160f9bb23a3d58a0accdbe89da13b96b1ece9", // SSH signature
			signedByGitalyOid,                          // has signature created by Gitaly
		},
	}

	expectedSignatures := []*gitalypb.GetCommitSignaturesResponse{
		{
			CommitId:   "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
			Signature:  testhelper.MustReadFile(t, "testdata/commit-5937ac0a7beb003549fc5fd26fc247adbce4a52e-signature"),
			SignedText: testhelper.MustReadFile(t, "testdata/commit-5937ac0a7beb003549fc5fd26fc247adbce4a52e-signed-text"),
			Signer:     gitalypb.GetCommitSignaturesResponse_SIGNER_USER,
		},
		{
			CommitId:   "a17a9f66543673edf0a3d1c6b93bdda3fe600f32",
			Signature:  testhelper.MustReadFile(t, "testdata/gitlab-test-commit-a17a9f66543673edf0a3d1c6b93bdda3fe600f32-signature"),
			SignedText: testhelper.MustReadFile(t, "testdata/gitlab-test-commit-a17a9f66543673edf0a3d1c6b93bdda3fe600f32-signed-text"),
			Signer:     gitalypb.GetCommitSignaturesResponse_SIGNER_USER,
		},
		{
			CommitId:   "8cf8e80a5a0546e391823c250f2b26b9cf15ce88",
			Signature:  testhelper.MustReadFile(t, "testdata/gitaly-test-commit-8cf8e80a5a0546e391823c250f2b26b9cf15ce88-signature"),
			SignedText: testhelper.MustReadFile(t, "testdata/gitaly-test-commit-8cf8e80a5a0546e391823c250f2b26b9cf15ce88-signed-text"),
			Signer:     gitalypb.GetCommitSignaturesResponse_SIGNER_USER,
		},
		{
			CommitId:   "dc00eb001f41dfac08192ead79c2377c588b82ee",
			Signature:  testhelper.MustReadFile(t, "testdata/dc00eb001f41dfac08192ead79c2377c588b82ee-signed-no-newline-signature.txt"),
			SignedText: testhelper.MustReadFile(t, "testdata/dc00eb001f41dfac08192ead79c2377c588b82ee-signed-no-newline-signed-text.txt"),
			Signer:     gitalypb.GetCommitSignaturesResponse_SIGNER_USER,
		},
		{
			CommitId:   "7b5160f9bb23a3d58a0accdbe89da13b96b1ece9",
			Signature:  testhelper.MustReadFile(t, "testdata/7b5160f9bb23a3d58a0accdbe89da13b96b1ece9-ssh-signature"),
			SignedText: testhelper.MustReadFile(t, "testdata/7b5160f9bb23a3d58a0accdbe89da13b96b1ece9-ssh-signed-text"),
			Signer:     gitalypb.GetCommitSignaturesResponse_SIGNER_USER,
		},
	}

	if featureflag.GPGSigning.IsEnabled(ctx) {
		expectedSignatures = append(expectedSignatures, &gitalypb.GetCommitSignaturesResponse{
			CommitId:   signedByGitalyOid,
			Signature:  testhelper.MustReadFile(t, "testdata/38176a00dc2c373ee45dfb5d0e3c459ea46ac5b5-ssh-gitaly-signature"),
			SignedText: testhelper.MustReadFile(t, "testdata/38176a00dc2c373ee45dfb5d0e3c459ea46ac5b5-ssh-gitaly-signed-text"),
			Signer:     gitalypb.GetCommitSignaturesResponse_SIGNER_SYSTEM,
		})
	}

	c, err := client.GetCommitSignatures(ctx, request)
	require.NoError(t, err)

	fetchedSignatures := readAllSignaturesFromClient(t, c)

	require.Len(t, fetchedSignatures, len(expectedSignatures))
	for i, expected := range expectedSignatures {
		// We cannot use `testhelper.ProtoEqual` here due to it being too inefficient with
		// the data we're comparing because it contains multiple MB of signed data. This has
		// in the past led to frequent timeouts in CI.
		require.Equal(t, expected.CommitId, fetchedSignatures[i].CommitId)
		require.Equal(t, expected.Signature, fetchedSignatures[i].Signature)
		require.Equal(t, expected.SignedText, fetchedSignatures[i].SignedText)
		require.Equal(t, expected.Signer, fetchedSignatures[i].Signer)
	}
}

func TestFailedGetCommitSignaturesRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupCommitServiceWithRepo(t, ctx)

	testCases := []struct {
		desc        string
		request     *gitalypb.GetCommitSignaturesRequest
		expectedErr error
	}{
		{
			desc: "no repository provided",
			request: &gitalypb.GetCommitSignaturesRequest{
				Repository: nil,
				CommitIds:  []string{"5937ac0a7beb003549fc5fd26fc247adbce4a52e"},
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty CommitIds",
			request: &gitalypb.GetCommitSignaturesRequest{
				Repository: repo,
				CommitIds:  []string{},
			},
			expectedErr: status.Error(codes.InvalidArgument, "empty CommitIds"),
		},
		{
			desc: "commitIDS with shorthand sha",
			request: &gitalypb.GetCommitSignaturesRequest{
				Repository: repo,
				CommitIds:  []string{"5937ac0a7beb003549fc5fd26fc247adbce4a52e", "a17a9f6"},
			},
			expectedErr: status.Error(codes.InvalidArgument, fmt.Sprintf(`invalid object ID: "a17a9f6", expected length %v, got 7`, gittest.DefaultObjectHash.EncodedLen())),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			c, err := client.GetCommitSignatures(ctx, testCase.request)
			require.NoError(t, err)

			for {
				_, err = c.Recv()
				if err != nil {
					break
				}
			}

			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}

func readAllSignaturesFromClient(t *testing.T, c gitalypb.CommitService_GetCommitSignaturesClient) (signatures []*gitalypb.GetCommitSignaturesResponse) {
	t.Helper()

	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		if resp.CommitId != "" {
			signatures = append(signatures, resp)
			// first message contains either Signature or SignedText so no need to append anything
			continue
		}

		currentSignature := signatures[len(signatures)-1]

		if len(resp.Signature) != 0 {
			currentSignature.Signature = append(currentSignature.Signature, resp.Signature...)
		} else if len(resp.SignedText) != 0 {
			currentSignature.SignedText = append(currentSignature.SignedText, resp.SignedText...)
		}
	}

	return
}

func commitSignedByGitaly(t *testing.T, ctx context.Context, repoProto *gitalypb.Repository, cfg config.Cfg) string {
	testcfg.BuildGitalyGPG(t, cfg)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	blobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("updated"))
	require.NoError(t, err)

	tree := &localrepo.TreeEntry{
		Type: localrepo.Tree,
		Mode: "040000",
		Entries: []*localrepo.TreeEntry{
			{Path: "file", Mode: "100644", OID: blobID},
		},
	}
	require.NoError(t, tree.Write(ctx, repo))

	commitCfg := localrepo.WriteCommitConfig{
		TreeID:         tree.OID,
		AuthorName:     gittest.DefaultCommitterName,
		AuthorEmail:    gittest.DefaultCommitterMail,
		CommitterName:  gittest.DefaultCommitterName,
		CommitterEmail: gittest.DefaultCommitterMail,
		AuthorDate:     gittest.DefaultCommitTime,
		CommitterDate:  gittest.DefaultCommitTime,
		Message:        "message",
		SigningKey:     cfg.Git.SigningKey,
	}

	oid, err := repo.WriteCommit(ctx, commitCfg)
	require.Nil(t, err)

	return oid.String()
}
