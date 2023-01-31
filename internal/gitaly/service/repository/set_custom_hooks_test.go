//go:build !gitaly_test_sha256

package repository

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
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

func TestSetCustomHooksRequest_success(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TransactionalRestoreCustomHooks).
		Run(t, testSuccessfulSetCustomHooksRequest)
}

func testSuccessfulSetCustomHooksRequest(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamWriter func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) (io.Writer, func())
	}{
		{
			desc: "SetCustomHooks",
			streamWriter: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) (io.Writer, func()) {
				stream, err := client.SetCustomHooks(ctx)
				require.NoError(t, err)

				request := &gitalypb.SetCustomHooksRequest{Repository: repo}
				writer := streamio.NewWriter(func(p []byte) error {
					request.Data = p
					if err := stream.Send(request); err != nil {
						return err
					}

					request = &gitalypb.SetCustomHooksRequest{}
					return nil
				})

				closeFunc := func() {
					_, err = stream.CloseAndRecv()
					require.NoError(t, err)
				}

				return writer, closeFunc
			},
		},
		{
			desc: "RestoreCustomHooks",
			streamWriter: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) (io.Writer, func()) {
				//nolint:staticcheck
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

				closeFunc := func() {
					_, err = stream.CloseAndRecv()
					require.NoError(t, err)
				}

				return writer, closeFunc
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
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

			writer, closeStream := tc.streamWriter(t, ctx, repo, client)

			archivePath := mustCreateCustomHooksArchive(t, ctx, []testFile{
				{name: "pre-commit.sample", content: "foo", mode: 0o755},
				{name: "pre-push.sample", content: "bar", mode: 0o755},
			}, customHooksDir)

			file, err := os.Open(archivePath)
			require.NoError(t, err)

			_, err = io.Copy(writer, file)
			require.NoError(t, err)
			closeStream()

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
		})
	}
}

func TestSetCustomHooks_failedValidation(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TransactionalRestoreCustomHooks).
		Run(t, testFailedSetCustomHooksDueToValidations)
}

func testFailedSetCustomHooksDueToValidations(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamSender func(*testing.T, context.Context, gitalypb.RepositoryServiceClient) error
	}{
		{
			desc: "SetCustomHooks",
			streamSender: func(t *testing.T, ctx context.Context, client gitalypb.RepositoryServiceClient) error {
				stream, err := client.SetCustomHooks(ctx)
				require.NoError(t, err)

				require.NoError(t, stream.Send(&gitalypb.SetCustomHooksRequest{}))

				_, err = stream.CloseAndRecv()
				return err
			},
		},
		{
			desc: "RestoreCustomHooks",
			streamSender: func(t *testing.T, ctx context.Context, client gitalypb.RepositoryServiceClient) error {
				//nolint:staticcheck
				stream, err := client.RestoreCustomHooks(ctx)
				require.NoError(t, err)

				require.NoError(t, stream.Send(&gitalypb.RestoreCustomHooksRequest{}))

				_, err = stream.CloseAndRecv()
				return err
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, client := setupRepositoryServiceWithoutRepo(t)

			err := tc.streamSender(t, ctx, client)
			testhelper.RequireGrpcError(t, err, status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"validating repo: empty Repository",
				"repo scoped: empty Repository",
			)))
		})
	}
}

func TestSetCustomHooks_corruptTar(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TransactionalRestoreCustomHooks).
		Run(t, testFailedSetCustomHooksDueToBadTar)
}

func testFailedSetCustomHooksDueToBadTar(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		streamWriter func(*testing.T, context.Context, *gitalypb.Repository, gitalypb.RepositoryServiceClient) (io.Writer, func() error)
	}{
		{
			desc: "SetCustomHooks",
			streamWriter: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) (io.Writer, func() error) {
				stream, err := client.SetCustomHooks(ctx)
				require.NoError(t, err)

				request := &gitalypb.SetCustomHooksRequest{Repository: repo}
				writer := streamio.NewWriter(func(p []byte) error {
					request.Data = p
					if err := stream.Send(request); err != nil {
						return err
					}

					request = &gitalypb.SetCustomHooksRequest{}
					return nil
				})

				closeFunc := func() error {
					_, err = stream.CloseAndRecv()
					return err
				}

				return writer, closeFunc
			},
		},
		{
			desc: "RestoreCustomHooks",
			streamWriter: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, client gitalypb.RepositoryServiceClient) (io.Writer, func() error) {
				//nolint:staticcheck
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

				closeFunc := func() error {
					_, err = stream.CloseAndRecv()
					return err
				}

				return writer, closeFunc
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, repo, _, client := setupRepositoryService(t, ctx)
			writer, closeStream := tc.streamWriter(t, ctx, repo, client)

			archivePath := mustCreateCorruptHooksArchive(t)

			file, err := os.Open(archivePath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, file)

			_, err = io.Copy(writer, file)
			require.NoError(t, err)
			err = closeStream()
			testhelper.RequireGrpcCode(t, err, codes.Internal)
		})
	}
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
				{name: "pre-commit.sample", content: "foo", mode: perm.SharedExecutable},
				{name: "pre-push.sample", content: "bar", mode: perm.SharedExecutable},
			},
			expectedHash: "8ca11991268de4c9278488a674fc1a88db449566",
		},
		{
			desc: "generated hash matches with changed file name",
			files: []testFile{
				{name: "pre-commit.sample.diff", content: "foo", mode: perm.SharedExecutable},
				{name: "pre-push.sample", content: "bar", mode: perm.SharedExecutable},
			},
			expectedHash: "b5ed58ced84103da1ed9d7813a9e39b3b5daf7d7",
		},
		{
			desc: "generated hash matches with changed file content",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: perm.SharedExecutable},
				{name: "pre-push.sample", content: "bar.diff", mode: perm.SharedExecutable},
			},
			expectedHash: "178083848c8a08e36c4f86c2d318a84b0bb845f2",
		},
		{
			desc: "generated hash matches with changed file mode",
			files: []testFile{
				{name: "pre-commit.sample", content: "foo", mode: perm.SharedFile},
				{name: "pre-push.sample", content: "bar", mode: perm.SharedExecutable},
			},
			expectedHash: "c69574241b83496bb4005b4f7a0dfcda96cb317e",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			path := mustWriteCustomHookDirectory(t, tc.files, customHooksDir)

			voteHash, err := newDirectoryVote(path)
			require.NoError(t, err)

			vote, err := voteHash.Vote()
			require.NoError(t, err)

			hash := vote.String()
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}

func mustWriteCustomHookDirectory(t *testing.T, files []testFile, dirName string) string {
	t.Helper()

	tmpDir := testhelper.TempDir(t)
	hooksPath := filepath.Join(tmpDir, dirName)

	err := os.Mkdir(hooksPath, perm.SharedDir)
	require.NoError(t, err)

	for _, f := range files {
		err = os.WriteFile(filepath.Join(hooksPath, f.name), []byte(f.content), f.mode)
		require.NoError(t, err)
	}

	return hooksPath
}

func mustCreateCustomHooksArchive(t *testing.T, ctx context.Context, files []testFile, dirName string) string {
	t.Helper()

	hooksPath := mustWriteCustomHookDirectory(t, files, dirName)
	hooksDir := filepath.Dir(hooksPath)

	tmpDir := testhelper.TempDir(t)
	archivePath := filepath.Join(tmpDir, "custom_hooks.tar")

	file, err := os.Create(archivePath)
	require.NoError(t, err)

	err = archive.WriteTarball(ctx, file, hooksDir, dirName)
	require.NoError(t, err)

	return archivePath
}

func mustCreateCorruptHooksArchive(t *testing.T) string {
	t.Helper()

	tmpDir := testhelper.TempDir(t)
	archivePath := filepath.Join(tmpDir, "corrupt_hooks.tar")

	err := os.WriteFile(archivePath, []byte("This is a corrupted tar file"), 0o755)
	require.NoError(t, err)

	return archivePath
}

func TestExtractHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	hooksFiles := []testFile{
		{name: "pre-commit.sample", content: "foo", mode: 0o755},
		{name: "pre-push.sample", content: "bar", mode: 0o755},
	}

	tmpDir := testhelper.TempDir(t)
	emptyTar := filepath.Join(tmpDir, "empty_hooks.tar")
	err := os.WriteFile(emptyTar, nil, 0o644)
	require.NoError(t, err)

	for _, tc := range []struct {
		desc          string
		path          string
		expectedFiles []testFile
		expectedErr   string
	}{
		{
			desc: "custom hooks tar",
			path: mustCreateCustomHooksArchive(t, ctx, hooksFiles, customHooksDir),
			expectedFiles: []testFile{
				{name: "custom_hooks"},
				{name: "custom_hooks/pre-commit.sample", content: "foo"},
				{name: "custom_hooks/pre-push.sample", content: "bar"},
			},
		},
		{
			desc:          "empty custom hooks tar",
			path:          mustCreateCustomHooksArchive(t, ctx, []testFile{}, customHooksDir),
			expectedFiles: []testFile{{name: "custom_hooks"}},
		},
		{
			desc: "no hooks tar",
			path: mustCreateCustomHooksArchive(t, ctx, hooksFiles, "no_hooks"),
		},
		{
			desc:        "corrupt tar",
			path:        mustCreateCorruptHooksArchive(t),
			expectedErr: "waiting for tar command completion: exit status",
		},
		{
			desc: "empty tar",
			path: emptyTar,
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})

			tarball, err := os.Open(tc.path)
			require.NoError(t, err)
			defer tarball.Close()

			err = extractHooks(ctx, tarball, repoPath)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErr)
			}

			if len(tc.expectedFiles) == 0 {
				require.NoDirExists(t, filepath.Join(repoPath, customHooksDir))
				return
			}

			var extractedFiles []testFile

			hooksPath := filepath.Join(repoPath, customHooksDir)
			err = filepath.WalkDir(hooksPath, func(path string, d fs.DirEntry, err error) error {
				require.NoError(t, err)

				relPath, err := filepath.Rel(repoPath, path)
				require.NoError(t, err)

				var hookData []byte
				if !d.IsDir() {
					hookData, err = os.ReadFile(path)
					require.NoError(t, err)
				}

				extractedFiles = append(extractedFiles, testFile{
					name:    relPath,
					content: string(hookData),
				})

				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.expectedFiles, extractedFiles)
		})
	}
}
