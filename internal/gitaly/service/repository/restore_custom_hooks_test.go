//go:build !gitaly_test_sha256

package repository

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulRestoreCustomHooksRequest(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TransactionalRestoreCustomHooks).
		Run(t, testSuccessfulRestoreCustomHooksRequest)
}

func testSuccessfulRestoreCustomHooksRequest(t *testing.T, ctx context.Context) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	txManager := transaction.NewTrackingManager()

	client, addr := runRepositoryService(t, cfg, nil, testserver.WithTransactionManager(txManager))
	cfg.SocketPath = addr

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)

	ctx = metadata.IncomingToOutgoing(ctx)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// reset the txManager since CreateRepository would have done
	// voting
	txManager.Reset()
	stream, err := client.RestoreCustomHooks(ctx)
	require.NoError(t, err)

	request := &gitalypb.RestoreCustomHooksRequest{Repository: repo}

	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.RestoreCustomHooksRequest{}
		return nil
	})

	file, err := os.Open("testdata/custom_hooks.tar")
	require.NoError(t, err)

	_, err = io.Copy(writer, file)
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	voteHash, err := newDirectoryVote(filepath.Join(repoPath, customHooksDir))
	require.NoError(t, err)

	testhelper.MustClose(t, file)

	expectedVote, err := voteHash.Vote()
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(repoPath, "custom_hooks", "pre-push.sample"))

	if featureflag.TransactionalRestoreCustomHooks.IsEnabled(ctx) {
		require.Equal(t, 2, len(txManager.Votes()))
		assert.Equal(t, voting.Prepared, txManager.Votes()[0].Phase)
		assert.Equal(t, expectedVote, txManager.Votes()[1].Vote)
		assert.Equal(t, voting.Committed, txManager.Votes()[1].Phase)
	} else {
		require.Equal(t, 0, len(txManager.Votes()))
	}
}

func TestFailedRestoreCustomHooksDueToValidations(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TransactionalRestoreCustomHooks).
		Run(t, testFailedRestoreCustomHooksDueToValidations)
}

func testFailedRestoreCustomHooksDueToValidations(t *testing.T, ctx context.Context) {
	t.Parallel()
	_, client := setupRepositoryServiceWithoutRepo(t)

	stream, err := client.RestoreCustomHooks(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.RestoreCustomHooksRequest{}))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
		"empty Repository",
		"repo scoped: empty Repository",
	)))
}

func TestFailedRestoreCustomHooksDueToBadTar(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TransactionalRestoreCustomHooks).
		Run(t, testFailedRestoreCustomHooksDueToBadTar)
}

func testFailedRestoreCustomHooksDueToBadTar(t *testing.T, ctx context.Context) {
	_, repo, _, client := setupRepositoryService(t, ctx)

	stream, err := client.RestoreCustomHooks(ctx)

	require.NoError(t, err)

	request := &gitalypb.RestoreCustomHooksRequest{Repository: repo}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.RestoreCustomHooksRequest{}
		return nil
	})

	file, err := os.Open("testdata/corrupted_hooks.tar")
	require.NoError(t, err)
	defer file.Close()

	_, err = io.Copy(writer, file)
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()

	testhelper.RequireGrpcCode(t, err, codes.Internal)
}

type testFile struct {
	name    string
	content string
	mode    os.FileMode
}

func TestNewDirectoryVote(t *testing.T) {
	// The vote hash depends on the permission bits, so we must make sure that the files we
	// write have the same permission bits on all systems. As the umask can get in our way we
	// reset it to a known value here and restore it after the test. This also means that we
	// cannot parallelize this test.
	currentUmask := syscall.Umask(0)
	defer func() {
		syscall.Umask(currentUmask)
	}()
	syscall.Umask(0o022)

	for _, tc := range []struct {
		desc         string
		files        []testFile
		expectedHash string
	}{
		{
			desc: "generated hash matches",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: 0o755},
				{name: "pre-push.sample", content: "bar", mode: 0o755},
			},
			expectedHash: "8ca11991268de4c9278488a674fc1a88db449566",
		},
		{
			desc: "generated hash matches with changed file name",
			files: []testFile{
				{name: "pre-commit.sample.diff", content: "foo", mode: 0o755},
				{name: "pre-push.sample", content: "bar", mode: 0o755},
			},
			expectedHash: "b5ed58ced84103da1ed9d7813a9e39b3b5daf7d7",
		},
		{
			desc: "generated hash matches with changed file content",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: 0o755},
				{name: "pre-push.sample", content: "bar.diff", mode: 0o755},
			},
			expectedHash: "178083848c8a08e36c4f86c2d318a84b0bb845f2",
		},
		{
			desc: "generated hash matches with changed file mode",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: 0o644},
				{name: "pre-push.sample", content: "bar", mode: 0o755},
			},
			expectedHash: "c69574241b83496bb4005b4f7a0dfcda96cb317e",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			path := setupTestHooks(t, tc.files)

			voteHash, err := newDirectoryVote(path)
			require.NoError(t, err)

			vote, err := voteHash.Vote()
			require.NoError(t, err)

			hash := vote.String()
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}

func setupTestHooks(t *testing.T, files []testFile) string {
	t.Helper()

	tmpDir := testhelper.TempDir(t)
	hooksPath := filepath.Join(tmpDir, customHooksDir)

	err := os.Mkdir(hooksPath, 0o755)
	require.NoError(t, err)

	for _, f := range files {
		err = os.WriteFile(filepath.Join(hooksPath, f.name), []byte(f.content), f.mode)
		require.NoError(t, err)
	}

	return hooksPath
}
