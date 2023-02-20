//go:build !gitaly_test_sha256

package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestManager_RemoveAllRepositories(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision())

	pool := client.NewPool()
	defer testhelper.MustClose(t, pool)

	backupRoot := testhelper.TempDir(t)
	sink := NewFilesystemSink(backupRoot)
	locator, err := ResolveLocator("pointer", sink)
	require.NoError(t, err)

	fsBackup := NewManager(sink, locator, pool, backupID)
	err = fsBackup.RemoveAllRepositories(ctx, &RemoveAllRepositoriesRequest{
		Server:      storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
		StorageName: repo.StorageName,
	})
	require.NoError(t, err)
}

func TestManager_Create(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc               string
		setup              func(tb testing.TB) (*gitalypb.Repository, string)
		createsBundle      bool
		createsCustomHooks bool
		err                error
	}{
		{
			desc: "no hooks",
			setup: func(tb testing.TB) (*gitalypb.Repository, string) {
				noHooksRepo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})
				return noHooksRepo, repoPath
			},
			createsBundle:      true,
			createsCustomHooks: false,
		},
		{
			desc: "hooks",
			setup: func(tb testing.TB) (*gitalypb.Repository, string) {
				hooksRepo, hooksRepoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})
				require.NoError(tb, os.Mkdir(filepath.Join(hooksRepoPath, "custom_hooks"), perm.PublicDir))
				require.NoError(tb, os.WriteFile(filepath.Join(hooksRepoPath, "custom_hooks/pre-commit.sample"), []byte("Some hooks"), perm.PublicFile))
				return hooksRepo, hooksRepoPath
			},
			createsBundle:      true,
			createsCustomHooks: true,
		},
		{
			desc: "empty repo",
			setup: func(tb testing.TB) (*gitalypb.Repository, string) {
				emptyRepo, repoPath := gittest.CreateRepository(tb, ctx, cfg)
				return emptyRepo, repoPath
			},
			createsBundle:      false,
			createsCustomHooks: false,
			err:                fmt.Errorf("manager: repository empty: %w", ErrSkipped),
		},
		{
			desc: "nonexistent repo",
			setup: func(tb testing.TB) (*gitalypb.Repository, string) {
				emptyRepo, repoPath := gittest.CreateRepository(tb, ctx, cfg)
				nonexistentRepo := proto.Clone(emptyRepo).(*gitalypb.Repository)
				nonexistentRepo.RelativePath = gittest.NewRepositoryName(t)
				return nonexistentRepo, repoPath
			},
			createsBundle:      false,
			createsCustomHooks: false,
			err:                fmt.Errorf("manager: repository empty: %w", ErrSkipped),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repo, repoPath := tc.setup(t)
			backupRoot := testhelper.TempDir(t)

			refsPath := joinBackupPath(t, backupRoot, repo, backupID, "001.refs")
			bundlePath := joinBackupPath(t, backupRoot, repo, backupID, "001.bundle")
			customHooksPath := joinBackupPath(t, backupRoot, repo, backupID, "001.custom_hooks.tar")

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			sink := NewFilesystemSink(backupRoot)
			locator, err := ResolveLocator("pointer", sink)
			require.NoError(t, err)

			fsBackup := NewManager(sink, locator, pool, backupID)
			err = fsBackup.Create(ctx, &CreateRequest{
				Server:     storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
				Repository: repo,
			})
			if tc.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.err, err)
			}

			if tc.createsBundle {
				require.FileExists(t, refsPath)
				require.FileExists(t, bundlePath)

				dirInfo, err := os.Stat(filepath.Dir(bundlePath))
				require.NoError(t, err)
				require.Equal(t, perm.PrivateDir, dirInfo.Mode().Perm(), "expecting restricted directory permissions")

				bundleInfo, err := os.Stat(bundlePath)
				require.NoError(t, err)
				require.Equal(t, perm.PrivateFile, bundleInfo.Mode().Perm(), "expecting restricted file permissions")

				output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
				require.Contains(t, string(output), "The bundle records a complete history")

				expectedRefs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
				actualRefs := testhelper.MustReadFile(t, refsPath)
				require.Equal(t, string(expectedRefs), string(actualRefs))
			} else {
				require.NoFileExists(t, bundlePath)
			}

			if tc.createsCustomHooks {
				require.FileExists(t, customHooksPath)
			} else {
				require.NoFileExists(t, customHooksPath)
			}
		})
	}
}

func TestManager_Create_incremental(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc              string
		setup             func(t testing.TB, backupRoot string) (*gitalypb.Repository, string)
		expectedIncrement string
		expectedErr       error
	}{
		{
			desc: "no previous backup",
			setup: func(tb testing.TB, backupRoot string) (*gitalypb.Repository, string) {
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})
				return repo, repoPath
			},
			expectedIncrement: "001",
		},
		{
			desc: "previous backup, no updates",
			setup: func(tb testing.TB, backupRoot string) (*gitalypb.Repository, string) {
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				backupRepoPath := joinBackupPath(tb, backupRoot, repo)
				backupPath := filepath.Join(backupRepoPath, backupID)
				bundlePath := filepath.Join(backupPath, "001.bundle")
				refsPath := filepath.Join(backupPath, "001.refs")

				require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
				gittest.Exec(tb, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")

				refs := gittest.Exec(tb, cfg, "-C", repoPath, "show-ref", "--head")
				require.NoError(tb, os.WriteFile(refsPath, refs, perm.PublicFile))

				require.NoError(tb, os.WriteFile(filepath.Join(backupRepoPath, "LATEST"), []byte(backupID), perm.PublicFile))
				require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), perm.PublicFile))

				return repo, repoPath
			},
			expectedErr: fmt.Errorf("manager: write bundle: %w", fmt.Errorf("*backup.FilesystemSink write: %w: no changes to bundle", ErrSkipped)),
		},
		{
			desc: "previous backup, updates",
			setup: func(tb testing.TB, backupRoot string) (*gitalypb.Repository, string) {
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				backupRepoPath := joinBackupPath(tb, backupRoot, repo)
				backupPath := filepath.Join(backupRepoPath, backupID)
				bundlePath := filepath.Join(backupPath, "001.bundle")
				refsPath := filepath.Join(backupPath, "001.refs")

				require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
				gittest.Exec(tb, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")

				refs := gittest.Exec(tb, cfg, "-C", repoPath, "show-ref", "--head")
				require.NoError(tb, os.WriteFile(refsPath, refs, perm.PublicFile))

				require.NoError(tb, os.WriteFile(filepath.Join(backupRepoPath, "LATEST"), []byte(backupID), perm.PublicFile))
				require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), perm.PublicFile))

				gittest.WriteCommit(tb, cfg, repoPath, gittest.WithBranch("master"))

				return repo, repoPath
			},
			expectedIncrement: "002",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			backupRoot := testhelper.TempDir(t)
			repo, repoPath := tc.setup(t, backupRoot)

			refsPath := joinBackupPath(t, backupRoot, repo, backupID, tc.expectedIncrement+".refs")
			bundlePath := joinBackupPath(t, backupRoot, repo, backupID, tc.expectedIncrement+".bundle")

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			sink := NewFilesystemSink(backupRoot)
			locator, err := ResolveLocator("pointer", sink)
			require.NoError(t, err)

			fsBackup := NewManager(sink, locator, pool, backupID)
			err = fsBackup.Create(ctx, &CreateRequest{
				Server:      storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
				Repository:  repo,
				Incremental: true,
			})
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.FileExists(t, refsPath)
			require.FileExists(t, bundlePath)

			expectedRefs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
			actualRefs := testhelper.MustReadFile(t, refsPath)
			require.Equal(t, string(expectedRefs), string(actualRefs))
		})
	}
}

func TestManager_Restore(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TransactionalRestoreCustomHooks).
		Run(t, testManagerRestore)
}

func testManagerRestore(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	cc, err := client.Dial(cfg.SocketPath, nil)
	require.NoError(t, err)
	defer testhelper.MustClose(t, cc)

	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision())
	repoChecksum := gittest.ChecksumRepo(t, cfg, repoPath)

	backupRoot := testhelper.TempDir(t)

	for _, tc := range []struct {
		desc          string
		locators      []string
		setup         func(tb testing.TB) (*gitalypb.Repository, *git.Checksum)
		alwaysCreate  bool
		expectExists  bool
		expectedPaths []string
		expectedErrAs error
	}{
		{
			desc:     "existing repo, without hooks",
			locators: []string{"legacy", "pointer"},
			setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				relativePath := stripRelativePath(tb, repo)
				require.NoError(tb, os.MkdirAll(filepath.Join(backupRoot, relativePath), perm.PublicDir))
				bundlePath := filepath.Join(backupRoot, relativePath+".bundle")
				gittest.BundleRepo(tb, cfg, repoPath, bundlePath)

				return repo, repoChecksum
			},
			expectExists: true,
		},
		{
			desc:     "existing repo, with hooks",
			locators: []string{"legacy", "pointer"},
			setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				relativePath := stripRelativePath(tb, repo)
				bundlePath := filepath.Join(backupRoot, relativePath+".bundle")
				customHooksPath := filepath.Join(backupRoot, relativePath, "custom_hooks.tar")
				require.NoError(tb, os.MkdirAll(filepath.Join(backupRoot, relativePath), perm.PublicDir))
				gittest.BundleRepo(tb, cfg, repoPath, bundlePath)
				testhelper.CopyFile(tb, mustCreateCustomHooksArchive(t, ctx), customHooksPath)

				return repo, repoChecksum
			},
			expectedPaths: []string{
				"custom_hooks/pre-commit.sample",
				"custom_hooks/prepare-commit-msg.sample",
				"custom_hooks/pre-push.sample",
			},
			expectExists: true,
		},
		{
			desc:     "missing bundle",
			locators: []string{"legacy", "pointer"},
			setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				return repo, nil
			},
			expectedErrAs: ErrSkipped,
		},
		{
			desc:     "missing bundle, always create",
			locators: []string{"legacy", "pointer"},
			setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				return repo, new(git.Checksum)
			},
			alwaysCreate: true,
			expectExists: true,
		},
		{
			desc:     "nonexistent repo",
			locators: []string{"legacy", "pointer"},
			setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo := &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: gittest.NewRepositoryName(tb),
				}

				relativePath := stripRelativePath(tb, repo)
				require.NoError(tb, os.MkdirAll(filepath.Dir(filepath.Join(backupRoot, relativePath)), perm.PublicDir))
				bundlePath := filepath.Join(backupRoot, relativePath+".bundle")
				gittest.BundleRepo(tb, cfg, repoPath, bundlePath)

				return repo, repoChecksum
			},
			expectExists: true,
		},
		{
			desc:     "single incremental",
			locators: []string{"pointer"},
			setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
				const backupID = "abc123"
				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				repoBackupPath := joinBackupPath(tb, backupRoot, repo)
				backupPath := filepath.Join(repoBackupPath, backupID)
				require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
				require.NoError(tb, os.WriteFile(filepath.Join(repoBackupPath, "LATEST"), []byte(backupID), perm.PublicFile))
				require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), perm.PublicFile))
				bundlePath := filepath.Join(backupPath, "001.bundle")
				gittest.BundleRepo(tb, cfg, repoPath, bundlePath)

				return repo, repoChecksum
			},
			expectExists: true,
		},
		{
			desc:     "many incrementals",
			locators: []string{"pointer"},
			setup: func(tb testing.TB) (*gitalypb.Repository, *git.Checksum) {
				const backupID = "abc123"

				_, expectedRepoPath := gittest.CreateRepository(t, ctx, cfg)

				repo, _ := gittest.CreateRepository(t, ctx, cfg)
				repoBackupPath := joinBackupPath(tb, backupRoot, repo)
				backupPath := filepath.Join(repoBackupPath, backupID)
				require.NoError(tb, os.MkdirAll(backupPath, perm.PublicDir))
				require.NoError(tb, os.WriteFile(filepath.Join(repoBackupPath, "LATEST"), []byte(backupID), perm.PublicFile))
				require.NoError(tb, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("002"), perm.PublicFile))

				root := gittest.WriteCommit(tb, cfg, expectedRepoPath,
					gittest.WithBranch("master"),
				)
				master1 := gittest.WriteCommit(tb, cfg, expectedRepoPath,
					gittest.WithBranch("master"),
					gittest.WithParents(root),
				)
				other := gittest.WriteCommit(tb, cfg, expectedRepoPath,
					gittest.WithBranch("other"),
					gittest.WithParents(root),
				)
				gittest.Exec(tb, cfg, "-C", expectedRepoPath, "symbolic-ref", "HEAD", "refs/heads/master")
				bundlePath1 := filepath.Join(backupPath, "001.bundle")
				gittest.Exec(tb, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath1,
					"HEAD",
					"refs/heads/master",
					"refs/heads/other",
				)

				master2 := gittest.WriteCommit(tb, cfg, expectedRepoPath,
					gittest.WithBranch("master"),
					gittest.WithParents(master1),
				)
				bundlePath2 := filepath.Join(backupPath, "002.bundle")
				gittest.Exec(tb, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath2,
					"HEAD",
					"^"+master1.String(),
					"^"+other.String(),
					"refs/heads/master",
					"refs/heads/other",
				)

				checksum := new(git.Checksum)
				checksum.Add(git.NewReference("HEAD", master2.String()))
				checksum.Add(git.NewReference("refs/heads/master", master2.String()))
				checksum.Add(git.NewReference("refs/heads/other", other.String()))

				return repo, checksum
			},
			expectExists: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.GreaterOrEqual(t, len(tc.locators), 1, "each test case must specify a locator")

			for _, locatorName := range tc.locators {
				t.Run(locatorName, func(t *testing.T) {
					repo, expectedChecksum := tc.setup(t)

					pool := client.NewPool()
					defer testhelper.MustClose(t, pool)

					sink := NewFilesystemSink(backupRoot)
					locator, err := ResolveLocator(locatorName, sink)
					require.NoError(t, err)

					fsBackup := NewManager(sink, locator, pool, "unused-backup-id")
					err = fsBackup.Restore(ctx, &RestoreRequest{
						Server:       storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
						Repository:   repo,
						AlwaysCreate: tc.alwaysCreate,
					})
					if tc.expectedErrAs != nil {
						require.ErrorAs(t, err, &tc.expectedErrAs)
					} else {
						require.NoError(t, err)
					}

					exists, err := repoClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
						Repository: repo,
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectExists, exists.Exists, "repository exists")

					if expectedChecksum != nil {
						checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
							Repository: repo,
						})
						require.NoError(t, err)

						require.Equal(t, expectedChecksum.String(), checksum.GetChecksum())
					}

					if len(tc.expectedPaths) > 0 {
						// Restore has to use the rewritten path as the relative path due to the test creating
						// the repository through Praefect. In order to get to the correct disk paths, we need
						// to get the replica path of the rewritten repository.
						repoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, repo))
						for _, p := range tc.expectedPaths {
							require.FileExists(t, filepath.Join(repoPath, p))
						}
					}
				})
			}
		})
	}
}

func TestManager_CreateRestore_contextServerInfo(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.WriteTag(t, cfg, repoPath, "v1.0.0", commitID.Revision())

	backupRoot := testhelper.TempDir(t)

	pool := client.NewPool()
	defer testhelper.MustClose(t, pool)

	sink := NewFilesystemSink(backupRoot)
	locator, err := ResolveLocator("pointer", sink)
	require.NoError(t, err)

	fsBackup := NewManager(sink, locator, pool, "unused-backup-id")

	ctx = testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	require.NoError(t, fsBackup.Create(ctx, &CreateRequest{
		Repository: repo,
	}))
	require.NoError(t, fsBackup.Restore(ctx, &RestoreRequest{
		Repository: repo,
	}))
}

func TestResolveSink(t *testing.T) {
	ctx := testhelper.Context(t)

	isStorageServiceSink := func(expErrMsg string) func(t *testing.T, sink Sink) {
		return func(t *testing.T, sink Sink) {
			t.Helper()
			sssink, ok := sink.(*StorageServiceSink)
			require.True(t, ok)
			_, err := sssink.bucket.List(nil).Next(ctx)
			ierr, ok := err.(interface{ Unwrap() error })
			require.True(t, ok)
			terr := ierr.Unwrap()
			require.Contains(t, terr.Error(), expErrMsg)
		}
	}

	tmpDir := testhelper.TempDir(t)
	gsCreds := filepath.Join(tmpDir, "gs.creds")
	require.NoError(t, os.WriteFile(gsCreds, []byte(`
{
  "type": "service_account",
  "project_id": "hostfactory-179005",
  "private_key_id": "6253b144ccd94f50ce1224a73ffc48bda256d0a7",
  "private_key": "-----BEGIN PRIVATE KEY-----\nXXXX<KEY CONTENT OMMIT HERR> \n-----END PRIVATE KEY-----\n",
  "client_email": "303721356529-compute@developer.gserviceaccount.com",
  "client_id": "116595416948414952474",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/303724477529-compute%40developer.gserviceaccount.com"
}`), perm.SharedFile))

	for _, tc := range []struct {
		desc   string
		envs   map[string]string
		path   string
		verify func(t *testing.T, sink Sink)
		errMsg string
	}{
		{
			desc: "AWS S3",
			envs: map[string]string{
				"AWS_ACCESS_KEY_ID":     "test",
				"AWS_SECRET_ACCESS_KEY": "test",
				"AWS_REGION":            "us-east-1",
			},
			path:   "s3://bucket",
			verify: isStorageServiceSink("The AWS Access Key Id you provided does not exist in our records."),
		},
		{
			desc: "Google Cloud Storage",
			envs: map[string]string{
				"GOOGLE_APPLICATION_CREDENTIALS": gsCreds,
			},
			path:   "blob+gs://bucket",
			verify: isStorageServiceSink("storage.googleapis.com"),
		},
		{
			desc: "Azure Cloud File Storage",
			envs: map[string]string{
				"AZURE_STORAGE_ACCOUNT":   "test",
				"AZURE_STORAGE_KEY":       "test",
				"AZURE_STORAGE_SAS_TOKEN": "test",
			},
			path:   "blob+bucket+azblob://bucket",
			verify: isStorageServiceSink("https://test.blob.core.windows.net"),
		},
		{
			desc: "Filesystem",
			path: "/some/path",
			verify: func(t *testing.T, sink Sink) {
				require.IsType(t, &FilesystemSink{}, sink)
			},
		},
		{
			desc:   "undefined",
			path:   "some:invalid:path\x00",
			errMsg: `parse "some:invalid:path\x00": net/url: invalid control character in URL`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for k, v := range tc.envs {
				t.Setenv(k, v)
			}

			sink, err := ResolveSink(ctx, tc.path)
			if tc.errMsg != "" {
				require.EqualError(t, err, tc.errMsg)
				return
			}
			tc.verify(t, sink)
		})
	}
}

func TestResolveLocator(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		layout      string
		expectedErr string
	}{
		{layout: "legacy"},
		{layout: "pointer"},
		{
			layout:      "unknown",
			expectedErr: "unknown layout: \"unknown\"",
		},
	} {
		t.Run(tc.layout, func(t *testing.T) {
			l, err := ResolveLocator(tc.layout, nil)

			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
				return
			}

			require.NotNil(t, l)
		})
	}
}

func joinBackupPath(tb testing.TB, backupRoot string, repo *gitalypb.Repository, elements ...string) string {
	return filepath.Join(append([]string{
		backupRoot,
		stripRelativePath(tb, repo),
	}, elements...)...)
}

func stripRelativePath(tb testing.TB, repo *gitalypb.Repository) string {
	return strings.TrimSuffix(repo.GetRelativePath(), ".git")
}

func mustCreateCustomHooksArchive(t *testing.T, ctx context.Context) string {
	t.Helper()

	tmpDir := testhelper.TempDir(t)

	hooksDirPath := filepath.Join(tmpDir, "custom_hooks")
	require.NoError(t, os.Mkdir(hooksDirPath, os.ModePerm))

	require.NoError(t, os.WriteFile(filepath.Join(hooksDirPath, "pre-commit.sample"), []byte("foo"), os.ModePerm))
	require.NoError(t, os.WriteFile(filepath.Join(hooksDirPath, "prepare-commit-msg.sample"), []byte("bar"), os.ModePerm))
	require.NoError(t, os.WriteFile(filepath.Join(hooksDirPath, "pre-push.sample"), []byte("baz"), os.ModePerm))

	archivePath := filepath.Join(tmpDir, "custom_hooks.tar")
	file, err := os.Create(archivePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, file)

	require.NoError(t, archive.WriteTarball(ctx, file, tmpDir, "custom_hooks"))

	return archivePath
}
