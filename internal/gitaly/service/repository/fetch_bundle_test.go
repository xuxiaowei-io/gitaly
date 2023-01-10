//go:build !gitaly_test_sha256

package repository

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer_FetchBundle_success(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _, repoPath, client := setupRepositoryService(t, ctx)

	tmp := testhelper.TempDir(t)
	bundlePath := filepath.Join(tmp, "test.bundle")

	gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/feature")
	gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")
	expectedRefs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")

	targetRepo, targetRepoPath := gittest.CreateRepository(t, ctx, cfg)

	stream, err := client.FetchBundle(ctx)
	require.NoError(t, err)

	request := &gitalypb.FetchBundleRequest{Repository: targetRepo, UpdateHead: true}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.FetchBundleRequest{}

		return nil
	})

	bundle, err := os.Open(bundlePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, bundle)

	_, err = io.Copy(writer, bundle)
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	refs := gittest.Exec(t, cfg, "-C", targetRepoPath, "show-ref", "--head")
	require.Equal(t, string(expectedRefs), string(refs))
}

func TestServer_FetchBundle_transaction(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})

	hookManager := &mockHookManager{}
	client, _ := runRepositoryService(t, cfg, nil, testserver.WithHookManager(hookManager), testserver.WithDisablePraefect())

	tmp := testhelper.TempDir(t)
	bundlePath := filepath.Join(tmp, "test.bundle")
	gittest.BundleRepo(t, cfg, repoPath, bundlePath)

	hookManager.Reset()

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	require.Empty(t, hookManager.states)

	stream, err := client.FetchBundle(ctx)
	require.NoError(t, err)

	request := &gitalypb.FetchBundleRequest{Repository: repoProto}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.FetchBundleRequest{}

		return nil
	})

	bundle, err := os.Open(bundlePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, bundle)

	_, err = io.Copy(writer, bundle)
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, []gitalyhook.ReferenceTransactionState{
		gitalyhook.ReferenceTransactionPrepared,
		gitalyhook.ReferenceTransactionCommitted,
	}, hookManager.states)
}

func TestServer_FetchBundle_validation(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc         string
		firstRequest *gitalypb.FetchBundleRequest
		expectedErr  error
	}{
		{
			desc: "no repo",
			firstRequest: &gitalypb.FetchBundleRequest{
				Repository: nil,
			},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "unknown repo",
			firstRequest: &gitalypb.FetchBundleRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: "unknown",
				},
			},
			expectedErr: status.Error(codes.NotFound, testhelper.GitalyOrPraefect(
				`GetRepoPath: not a git repository: "`+cfg.Storages[0].Path+`/unknown"`,
				`mutator call: route repository mutator: get repository id: repository "default"/"unknown" not found`,
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.FetchBundle(ctx)
			require.NoError(t, err)

			err = stream.Send(tc.firstRequest)
			require.NoError(t, err)

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

type mockHookManager struct {
	gitalyhook.Manager
	states []gitalyhook.ReferenceTransactionState
}

func (m *mockHookManager) Reset() {
	m.states = make([]gitalyhook.ReferenceTransactionState, 0)
}

func (m *mockHookManager) ReferenceTransactionHook(_ context.Context, state gitalyhook.ReferenceTransactionState, _ []string, _ io.Reader) error {
	m.states = append(m.states, state)
	return nil
}
