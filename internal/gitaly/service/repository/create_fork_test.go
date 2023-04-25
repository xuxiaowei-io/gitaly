package repository

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/praefectutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	gitalyx509 "gitlab.com/gitlab-org/gitaly/v15/internal/x509"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateFork_successful(t *testing.T) {
	// We need to inject this once across all tests given that crypto/x509 only initializes
	// certificates once. Changing injected certs during our tests is thus not going to fly well
	// and would cause failure. We should eventually address this and provide better testing
	// utilities around this, but now's not the time.
	certFile, keyFile := testhelper.GenerateCerts(t)
	t.Setenv(gitalyx509.SSLCertFile, certFile)

	for _, tt := range []struct {
		name   string
		secure bool
	}{
		{
			name:   "secure",
			secure: true,
		},
		{
			name: "insecure",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testcfg.Build(t)

			testcfg.BuildGitalyHooks(t, cfg)
			testcfg.BuildGitalySSH(t, cfg)

			var client gitalypb.RepositoryServiceClient
			if tt.secure {
				cfg.TLS = config.TLS{
					CertPath: certFile,
					KeyPath:  keyFile,
				}
				cfg.TLSListenAddr = "localhost:0"

				client, cfg.TLSListenAddr = runRepositoryService(t, cfg)
			} else {
				client, cfg.SocketPath = runRepositoryService(t, cfg)
			}

			ctx := testhelper.Context(t)
			ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

			repo, _ := gittest.CreateRepository(t, ctx, cfg)

			forkedRepo := &gitalypb.Repository{
				RelativePath: gittest.NewRepositoryName(t),
				StorageName:  repo.GetStorageName(),
			}

			_, err := client.CreateFork(ctx, &gitalypb.CreateForkRequest{
				Repository:       forkedRepo,
				SourceRepository: repo,
			})
			require.NoError(t, err)

			replicaPath := gittest.GetReplicaPath(t, ctx, cfg, forkedRepo)
			forkedRepoPath := filepath.Join(cfg.Storages[0].Path, replicaPath)

			gittest.Exec(t, cfg, "-C", forkedRepoPath, "fsck")
			require.Empty(t, gittest.Exec(t, cfg, "-C", forkedRepoPath, "remote"))

			_, err = os.Lstat(filepath.Join(forkedRepoPath, "hooks"))
			require.True(t, os.IsNotExist(err), "hooks directory should not have been created")
		})
	}
}

func TestCreateFork_refs(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryServiceWithoutRepo(t)

	sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)

	// Prepare the source repository with a bunch of refs and a non-default HEAD ref so we can
	// assert that the target repo gets created with the correct set of refs.
	commitID := gittest.WriteCommit(t, cfg, sourceRepoPath)
	for _, ref := range []string{
		"refs/environments/something",
		"refs/heads/something",
		"refs/remotes/origin/something",
		"refs/tags/something",
	} {
		gittest.Exec(t, cfg, "-C", sourceRepoPath, "update-ref", ref, commitID.String())
	}
	gittest.Exec(t, cfg, "-C", sourceRepoPath, "symbolic-ref", "HEAD", "refs/heads/something")

	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	targetRepo := &gitalypb.Repository{
		RelativePath: gittest.NewRepositoryName(t),
		StorageName:  sourceRepo.GetStorageName(),
	}

	_, err := client.CreateFork(ctx, &gitalypb.CreateForkRequest{
		Repository:       targetRepo,
		SourceRepository: sourceRepo,
	})
	require.NoError(t, err)

	storagePath, err := config.NewLocator(cfg).GetStorageByName(targetRepo.GetStorageName())
	require.NoError(t, err)

	targetRepoPath := filepath.Join(storagePath, gittest.GetReplicaPath(t, ctx, cfg, targetRepo))

	require.Equal(t,
		[]string{
			commitID.String() + " refs/heads/something",
			commitID.String() + " refs/tags/something",
		},
		strings.Split(text.ChompBytes(gittest.Exec(t, cfg, "-C", targetRepoPath, "show-ref")), "\n"),
	)

	require.Equal(t,
		string(gittest.Exec(t, cfg, "-C", sourceRepoPath, "symbolic-ref", "HEAD")),
		string(gittest.Exec(t, cfg, "-C", targetRepoPath, "symbolic-ref", "HEAD")),
	)
}

func TestCreateFork_fsck(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryServiceWithoutRepo(t)

	ctx := testhelper.Context(t)
	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// Write a tree into the repository that's known-broken.
	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Content: "content", Path: "dup", Mode: "100644"},
		{Content: "content", Path: "dup", Mode: "100644"},
	})

	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(),
		gittest.WithBranch("main"),
		gittest.WithTree(treeID),
	)

	forkedRepo := &gitalypb.Repository{
		RelativePath: gittest.NewRepositoryName(t),
		StorageName:  repo.GetStorageName(),
	}

	// Create a fork from the repository with the broken tree. This should work alright: repos
	// with preexisting broken objects that we already have on our disk anyway should not be
	// subject to additional consistency checks. Otherwise we might end up in a situation where
	// we retroactively tighten consistency checks for repositories such that preexisting repos
	// wouldn't be forkable anymore.
	_, err := client.CreateFork(ctx, &gitalypb.CreateForkRequest{
		Repository:       forkedRepo,
		SourceRepository: repo,
	})
	require.NoError(t, err)

	forkedRepoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, forkedRepo))

	// Verify that the broken tree is indeed in the fork and that it is reported as broken by
	// git-fsck(1).
	var stderr bytes.Buffer
	fsckCmd := gittest.NewCommand(t, cfg, "-C", forkedRepoPath, "fsck")
	fsckCmd.Stderr = &stderr

	require.EqualError(t, fsckCmd.Run(), "exit status 4")
	require.Equal(t, fmt.Sprintf("error in tree %s: duplicateEntries: contains duplicate file entries\n", treeID), stderr.String())
}

func TestCreateFork_targetExists(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc                          string
		seed                          func(t *testing.T, targetPath string)
		expectedErrWithAtomicCreation error
	}{
		{
			desc: "empty target directory",
			seed: func(t *testing.T, targetPath string) {
				require.NoError(t, os.MkdirAll(targetPath, perm.GroupPrivateDir))
			},
			expectedErrWithAtomicCreation: structerr.NewAlreadyExists("creating fork: repository exists already"),
		},
		{
			desc: "non-empty target directory",
			seed: func(t *testing.T, targetPath string) {
				require.NoError(t, os.MkdirAll(targetPath, perm.GroupPrivateDir))
				require.NoError(t, os.WriteFile(
					filepath.Join(targetPath, "config"),
					nil,
					perm.SharedFile,
				))
			},
			expectedErrWithAtomicCreation: structerr.NewAlreadyExists("creating fork: repository exists already"),
		},
		{
			desc: "target file",
			seed: func(t *testing.T, targetPath string) {
				require.NoError(t, os.MkdirAll(filepath.Dir(targetPath), perm.GroupPrivateDir))
				require.NoError(t, os.WriteFile(targetPath, nil, perm.SharedFile))
			},
			expectedErrWithAtomicCreation: structerr.NewAlreadyExists("creating fork: repository exists already"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg, client := setupRepositoryServiceWithoutRepo(t)
			ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

			repo, _ := gittest.CreateRepository(t, ctx, cfg)

			forkedRepo := &gitalypb.Repository{
				// As this test can run with Praefect in front of it, we'll use the next replica path Praefect will
				// assign in order to ensure this repository creation conflicts even with Praefect in front of it.
				// As the source repository created in the setup is the first one, this would get the repository
				// ID 2.
				RelativePath: praefectutil.DeriveReplicaPath(2),
				StorageName:  repo.StorageName,
			}

			tc.seed(t, filepath.Join(cfg.Storages[0].Path, forkedRepo.GetRelativePath()))

			_, err := client.CreateFork(ctx, &gitalypb.CreateForkRequest{
				Repository:       forkedRepo,
				SourceRepository: repo,
			})
			testhelper.RequireGrpcError(t, tc.expectedErrWithAtomicCreation, err)
		})
	}
}

func TestCreateFork_validate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, cli := setupRepositoryServiceWithoutRepo(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.CreateForkRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			req:  &gitalypb.CreateForkRequest{Repository: nil, SourceRepository: repo},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "source repository not provided",
			req:  &gitalypb.CreateForkRequest{Repository: repo, SourceRepository: nil},
			expectedErr: func() error {
				if testhelper.IsPraefectEnabled() {
					return status.Error(codes.AlreadyExists, "route repository creation: reserve repository id: repository already exists")
				}
				return status.Error(codes.InvalidArgument, "validating source repository: empty Repository")
			}(),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := cli.CreateFork(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestCreateFork_linkToObjectPool(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t, testcfg.WithStorages("a", "b"))
	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalySSH(t, cfg)

	client, serverSocketPath := runRepositoryService(t, cfg)
	cfg.SocketPath = serverSocketPath

	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	type expectedState struct {
		alternates   []string
		looseObjects uint64
		packfiles    uint64
	}

	type setupData struct {
		request          *gitalypb.CreateForkRequest
		expectedErr      error
		expectedResponse *gitalypb.CreateForkResponse
		expectedState    expectedState
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "linking against unset pool fails",
			setup: func(t *testing.T) setupData {
				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.CreateForkRequest{
						Repository: &gitalypb.Repository{
							StorageName:  sourceRepo.StorageName,
							RelativePath: gittest.NewRepositoryName(t),
						},
						SourceRepository: sourceRepo,
						Mode: &gitalypb.CreateForkRequest_LinkToObjectPool{
							LinkToObjectPool: &gitalypb.CreateForkRequest_LinkToObjectPoolMode{
								ObjectPool: &gitalypb.ObjectPool{
									Repository: nil,
								},
							},
						},
					},
					expectedErr: structerr.NewInvalidArgument("validating object pool: empty Repository"),
				}
			},
		},
		{
			desc: "linking against pool on different storage fails",
			setup: func(t *testing.T) setupData {
				// We create the source repository and its object pool on the second
				// storage, but try to create the fork on the first storage.
				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					Storage: cfg.Storages[1],
				})
				objectPool, _ := gittest.CreateObjectPool(t, ctx, cfg, sourceRepo)

				return setupData{
					request: &gitalypb.CreateForkRequest{
						Repository: &gitalypb.Repository{
							StorageName:  cfg.Storages[0].Name,
							RelativePath: gittest.NewRepositoryName(t),
						},
						SourceRepository: sourceRepo,
						Mode: &gitalypb.CreateForkRequest_LinkToObjectPool{
							LinkToObjectPool: &gitalypb.CreateForkRequest_LinkToObjectPoolMode{
								ObjectPool: objectPool,
							},
						},
					},
					expectedErr: testhelper.GitalyOrPraefect(
						structerr.NewInvalidArgument("cannot link to object pool on different storage"),
						// Praefect fails in weird ways when trying to
						// resolve repositories with different storages.
						// This is something that we're fixing in a separate
						// track so that Praefect will fail gracefully with
						// such requests.
						structerr.NewNotFound(
							"route repository creation: resolve additional replica path: get additional repository id: repository %q/%q not found",
							cfg.Storages[0].Name,
							objectPool.Repository.GetRelativePath(),
						),
					),
				}
			},
		},
		{
			desc: "linking succeeds with unpooled repository",
			setup: func(t *testing.T) setupData {
				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)

				// We create an object pool, but the source repository is not linked
				// against it.
				objectPool, objectPoolPath := gittest.CreateObjectPool(t, ctx, cfg, sourceRepo, gittest.CreateObjectPoolConfig{
					LinkRepositoryToObjectPool: false,
				})

				return setupData{
					request: &gitalypb.CreateForkRequest{
						Repository: &gitalypb.Repository{
							StorageName:  sourceRepo.StorageName,
							RelativePath: gittest.NewRepositoryName(t),
						},
						SourceRepository: sourceRepo,
						Mode: &gitalypb.CreateForkRequest_LinkToObjectPool{
							LinkToObjectPool: &gitalypb.CreateForkRequest_LinkToObjectPoolMode{
								ObjectPool: objectPool,
							},
						},
					},
					expectedResponse: &gitalypb.CreateForkResponse{},
					expectedState: expectedState{
						alternates: []string{
							filepath.Join(objectPoolPath, "objects"),
						},
					},
				}
			},
		},
		{
			desc: "linking against missing pool fails",
			setup: func(t *testing.T) setupData {
				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)

				// We link the source repository to the object pool, but immediately
				// delete it afterwards.
				objectPool, objectPoolPath := gittest.CreateObjectPool(t, ctx, cfg, sourceRepo, gittest.CreateObjectPoolConfig{
					LinkRepositoryToObjectPool: true,
				})
				require.NoError(t, os.RemoveAll(objectPoolPath))

				return setupData{
					request: &gitalypb.CreateForkRequest{
						Repository: &gitalypb.Repository{
							StorageName:  sourceRepo.StorageName,
							RelativePath: gittest.NewRepositoryName(t),
						},
						SourceRepository: sourceRepo,
						Mode: &gitalypb.CreateForkRequest_LinkToObjectPool{
							LinkToObjectPool: &gitalypb.CreateForkRequest_LinkToObjectPoolMode{
								ObjectPool: objectPool,
							},
						},
					},
					expectedErr: structerr.NewFailedPrecondition("object pool of source repository is not a valid repository"),
				}
			},
		},
		{
			desc: "linking against empty object pool succeeds",
			setup: func(t *testing.T) setupData {
				sourceRepo, _ := gittest.CreateRepository(t, ctx, cfg)
				objectPool, objectPoolPath := gittest.CreateObjectPool(t, ctx, cfg, sourceRepo, gittest.CreateObjectPoolConfig{
					LinkRepositoryToObjectPool: true,
				})

				return setupData{
					request: &gitalypb.CreateForkRequest{
						Repository: &gitalypb.Repository{
							StorageName:  sourceRepo.StorageName,
							RelativePath: gittest.NewRepositoryName(t),
						},
						SourceRepository: sourceRepo,
						Mode: &gitalypb.CreateForkRequest_LinkToObjectPool{
							LinkToObjectPool: &gitalypb.CreateForkRequest_LinkToObjectPoolMode{
								ObjectPool: objectPool,
							},
						},
					},
					expectedResponse: &gitalypb.CreateForkResponse{},
					expectedState: expectedState{
						alternates: []string{
							filepath.Join(objectPoolPath, "objects"),
						},
					},
				}
			},
		},
		{
			desc: "objects deduplicated via object pool are not copied",
			setup: func(t *testing.T) setupData {
				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("branch"))

				objectPool, objectPoolPath := gittest.CreateObjectPool(t, ctx, cfg, sourceRepo, gittest.CreateObjectPoolConfig{
					LinkRepositoryToObjectPool: true,
				})

				return setupData{
					request: &gitalypb.CreateForkRequest{
						Repository: &gitalypb.Repository{
							StorageName:  sourceRepo.StorageName,
							RelativePath: gittest.NewRepositoryName(t),
						},
						SourceRepository: sourceRepo,
						Mode: &gitalypb.CreateForkRequest_LinkToObjectPool{
							LinkToObjectPool: &gitalypb.CreateForkRequest_LinkToObjectPoolMode{
								ObjectPool: objectPool,
							},
						},
					},
					expectedResponse: &gitalypb.CreateForkResponse{},
					expectedState: expectedState{
						alternates: []string{
							filepath.Join(objectPoolPath, "objects"),
						},
					},
				}
			},
		},
		{
			desc: "objects exclusive to the forked repository are copied",
			setup: func(t *testing.T) setupData {
				sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg)
				objectPool, objectPoolPath := gittest.CreateObjectPool(t, ctx, cfg, sourceRepo, gittest.CreateObjectPoolConfig{
					LinkRepositoryToObjectPool: true,
				})

				// Write the object after we have created the object pool so that
				// objects are exclusive to the pooled repository.
				gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("branch"))

				return setupData{
					request: &gitalypb.CreateForkRequest{
						Repository: &gitalypb.Repository{
							StorageName:  sourceRepo.StorageName,
							RelativePath: gittest.NewRepositoryName(t),
						},
						SourceRepository: sourceRepo,
						Mode: &gitalypb.CreateForkRequest_LinkToObjectPool{
							LinkToObjectPool: &gitalypb.CreateForkRequest_LinkToObjectPoolMode{
								ObjectPool: objectPool,
							},
						},
					},
					expectedResponse: &gitalypb.CreateForkResponse{},
					expectedState: expectedState{
						alternates: []string{
							filepath.Join(objectPoolPath, "objects"),
						},
						packfiles: 1,
					},
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			response, err := client.CreateFork(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, response)

			if err != nil {
				return
			}

			fork := localrepo.NewTestRepo(t, cfg, setup.request.Repository)
			// Assert that the repository is indeed valid.
			gittest.Exec(t, cfg, "-C", gittest.RepositoryPath(t, fork), "fsck")

			// Furthermore, assert that its contents look as expected.
			info, err := stats.RepositoryInfoForRepository(fork)
			require.NoError(t, err)
			info.Packfiles.LastFullRepack = time.Time{}
			require.Equal(t, setup.expectedState, expectedState{
				alternates:   info.Alternates,
				looseObjects: info.LooseObjects.Count,
				packfiles:    info.Packfiles.Count,
			})
		})
	}
}
