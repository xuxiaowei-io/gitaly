//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/client"
	gitalyerrors "gitlab.com/gitlab-org/gitaly/v16/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestReplicateRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "target"))
	cfg := cfgBuilder.Build(t)

	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalySSH(t, cfg)

	repoClient, serverSocketPath := runRepositoryService(t, cfg)
	cfg.SocketPath = serverSocketPath

	type setupData struct {
		source              *gitalypb.Repository
		target              *gitalypb.Repository
		expectedObjects     []string
		expectedCustomHooks []string
		expectedError       error
	}

	setupSourceAndTarget := func(t *testing.T, createTarget bool) (*gitalypb.Repository, string, *gitalypb.Repository, string) {
		t.Helper()

		source, sourcePath := gittest.CreateRepository(t, ctx, cfg)
		gittest.WriteCommit(t, cfg, sourcePath, gittest.WithMessage("init"), gittest.WithBranch("main"))

		var target *gitalypb.Repository
		var targetPath string
		if createTarget {
			target, targetPath = gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				RelativePath: source.GetRelativePath(),
				Storage:      cfg.Storages[1],
			})
		} else {
			target = proto.Clone(source).(*gitalypb.Repository)
			target.StorageName = cfg.Storages[1].Name
			targetPath = filepath.Join(cfg.Storages[1].Path, target.RelativePath)
		}

		return source, sourcePath, target, targetPath
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "replicate config",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, false)

				// Write a modified Git config file to the source repository to verify it is
				// getting created in the target repository as expected.
				gittest.Exec(t, cfg, "-C", sourcePath, "config", "please.replicate", "me")

				return setupData{
					source: source,
					target: target,
				}
			},
		},
		{
			desc: "replicate info attributes",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, false)

				// Write an info attributes file to the source repository to verify it is getting
				// created in the target repository as expected.
				attrFilePath := filepath.Join(sourcePath, "info", "attributes")
				require.NoError(t, os.MkdirAll(filepath.Dir(attrFilePath), perm.SharedDir))
				attributesData := []byte("*.pbxproj binary\n")
				require.NoError(t, os.WriteFile(attrFilePath, attributesData, perm.SharedFile))

				return setupData{
					source: source,
					target: target,
				}
			},
		},
		{
			desc: "replicate branch",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, false)

				// Create a new branch that only exists in the source repository to verify that it
				// is getting created in the target repository as expected.
				gittest.WriteCommit(t, cfg, sourcePath, gittest.WithBranch("branch"))

				return setupData{
					source: source,
					target: target,
				}
			},
		},
		{
			desc: "replicate unreachable object with snapshot-base replication",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, false)

				// Create a loose object in the source repository to verify that it is getting
				// created in the target repository as expected. This ensures that snapshot
				// replication is being used.
				blobData, err := text.RandomHex(10)
				require.NoError(t, err)
				blobID := text.ChompBytes(gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewBuffer([]byte(blobData))},
					"-C", sourcePath, "hash-object", "-w", "--stdin",
				))

				return setupData{
					source:          source,
					target:          target,
					expectedObjects: []string{blobID},
				}
			},
		},
		{
			desc: "replicate custom hooks",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, false)

				// Set up custom hooks in source repository to verify that the hooks are getting
				// created in the target repository as expected.
				hooks := testhelper.MustCreateCustomHooksTar(t)
				require.NoError(t, repoutil.ExtractHooks(ctx, hooks, sourcePath, false))

				return setupData{
					source:              source,
					target:              target,
					expectedCustomHooks: []string{"/pre-commit", "/pre-push", "/pre-receive"},
				}
			},
		},
		{
			desc: "replicate internal references to new target",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, false)

				// Create internal references in the source repository to verify that they are
				// getting created in the newly created target repository as expected. Regardless of
				// whether we classify them as hidden or read-only, we should be able to replicate
				// all of them.
				for refPrefix := range git.InternalRefPrefixes {
					_ = gittest.WriteCommit(t, cfg, sourcePath, gittest.WithParents(), gittest.WithMessage(refPrefix), gittest.WithReference(refPrefix+"1"))
				}

				return setupData{
					source: source,
					target: target,
				}
			},
		},
		{
			desc: "replicate internal references to existing target",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, targetPath := setupSourceAndTarget(t, true)

				// Create the same commit in both repositories so that they're in a known-good
				// state.
				sourceCommitID := gittest.WriteCommit(t, cfg, sourcePath, gittest.WithParents(), gittest.WithMessage("base"), gittest.WithBranch("main"))
				targetCommitID := gittest.WriteCommit(t, cfg, targetPath, gittest.WithParents(), gittest.WithMessage("base"), gittest.WithBranch("main"))
				require.Equal(t, sourceCommitID, targetCommitID)

				// Create internal references in the source repository to verify that they are
				// getting created in the existing target repository as expected. Regardless of
				// whether we classify them as hidden or read-only, we should be able to replicate
				// all of them.
				for refPrefix := range git.InternalRefPrefixes {
					gittest.WriteCommit(t, cfg, sourcePath, gittest.WithParents(), gittest.WithMessage(refPrefix), gittest.WithReference(refPrefix+"1"))
				}

				return setupData{
					source: source,
					target: target,
				}
			},
		},
		{
			desc: "empty target repository",
			setup: func(t *testing.T) setupData {
				source, _ := gittest.CreateRepository(t, ctx, cfg)

				// Set up a request with no target repository specified to verify that validation
				// fails and the RPC returns an error as expected.
				return setupData{
					source: source,
					target: nil,
					expectedError: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
						gitalyerrors.ErrEmptyRepository.Error(),
						fmt.Sprintf("repo scoped: %s", gitalyerrors.ErrEmptyRepository.Error()),
					)),
				}
			},
		},
		{
			desc: "empty source repository",
			setup: func(t *testing.T) setupData {
				// Set up a request with no source repository specified to verify that validation
				// fails and the RPC returns an error as expected.
				return setupData{
					source: nil,
					target: &gitalypb.Repository{
						StorageName:  cfg.Storages[1].Name,
						RelativePath: "/ab/cd/abcdef1234",
					},
					expectedError: structerr.NewInvalidArgument("source repository cannot be empty"),
				}
			},
		},
		{
			desc: "target and source repository have same storage",
			setup: func(t *testing.T) setupData {
				// Set up a request with a source and target repository that share the same storage
				// to verify that validation fails and the RPC returns an error as expected.
				source, _ := gittest.CreateRepository(t, ctx, cfg)
				target := proto.Clone(source).(*gitalypb.Repository)

				return setupData{
					source:        source,
					target:        target,
					expectedError: structerr.NewInvalidArgument("repository and source have the same storage"),
				}
			},
		},
		{
			desc: "invalid target repository",
			setup: func(t *testing.T) setupData {
				source, _, target, targetPath := setupSourceAndTarget(t, true)

				// Delete Git data to make target repository invalid to verify that replication
				// proceeds and the invalid target repository is overwritten. No error is expected
				// since the RPC is still able to successfully replicate.
				for _, path := range []string{"refs", "objects", "HEAD"} {
					require.NoError(t, os.RemoveAll(filepath.Join(targetPath, path)))
				}

				return setupData{
					source: source,
					target: target,
				}
			},
		},
		{
			desc: "invalid source repository",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, false)

				// Delete Git data to make the source repository invalid to verify that repository
				// validation fails on the source and the RPC returns an error as expected.
				for _, path := range []string{"refs", "objects", "HEAD"} {
					require.NoError(t, os.RemoveAll(filepath.Join(sourcePath, path)))
				}

				return setupData{
					source:        source,
					target:        target,
					expectedError: ErrInvalidSourceRepository,
				}
			},
		},
		{
			desc: "invalid source and target repository",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, targetPath := setupSourceAndTarget(t, true)

				// Delete Git data to make the source and target repositories invalid to verify that
				// repository validation fails and the RPC returns an error as expected.
				for _, repoPath := range []string{sourcePath, targetPath} {
					for _, path := range []string{"refs", "objects", "HEAD"} {
						require.NoError(t, os.RemoveAll(filepath.Join(repoPath, path)))
					}
				}

				return setupData{
					source:        source,
					target:        target,
					expectedError: ErrInvalidSourceRepository,
				}
			},
		},
		{
			desc: "fail to fetch source repository",
			setup: func(t *testing.T) setupData {
				source, sourcePath, target, _ := setupSourceAndTarget(t, true)

				// Corrupt the repository by writing garbage into HEAD to verify the RPC fails to
				// fetch the source repository and returns an error as expected.
				require.NoError(t, os.WriteFile(filepath.Join(sourcePath, "HEAD"), []byte("garbage"), perm.PublicFile))

				return setupData{
					source:        source,
					target:        target,
					expectedError: structerr.NewInternal("replicating repository: synchronizing references: fetch internal remote: exit status 128"),
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			ctx := testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))
			_, err := repoClient.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
				Repository: setup.target,
				Source:     setup.source,
			})

			// Verify error matches expected test case state.
			expectedStatus := status.Convert(setup.expectedError)
			actualStatus := status.Convert(err)

			// It is possible for the returned error to contain metadata that is difficult to assert
			// equivalency. For this reason, only the status code and error message are verified.
			require.Equal(t, expectedStatus.Code(), actualStatus.Code())
			require.Equal(t, expectedStatus.Message(), actualStatus.Message())
			if err != nil {
				return
			}

			sourcePath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(t, ctx, cfg, setup.source))
			targetPath := filepath.Join(cfg.Storages[1].Path, gittest.GetReplicaPath(t, ctx, cfg, setup.target))

			// Verify target repository connectivity.
			gittest.Exec(t, cfg, "-C", targetPath, "fsck")

			// Verify Git config matches.
			require.Equal(t,
				testhelper.MustReadFile(t, filepath.Join(sourcePath, "config")),
				testhelper.MustReadFile(t, filepath.Join(targetPath, "config")),
				"config file must match",
			)

			// Verify info attributes matches.
			sourceAttributesData, err := os.ReadFile(filepath.Join(sourcePath, "info", "attributes"))
			if err != nil {
				require.ErrorIs(t, err, os.ErrNotExist)
			}

			require.Equal(t,
				string(sourceAttributesData),
				string(testhelper.MustReadFile(t, filepath.Join(targetPath, "info", "attributes"))),
				"info/attributes file must match",
			)

			// Verify custom hooks replicated.
			var targetHooks []string
			targetHooksPath := filepath.Join(targetPath, repoutil.CustomHooksDir)
			require.NoError(t, filepath.WalkDir(targetHooksPath, func(path string, entry fs.DirEntry, err error) error {
				if os.IsNotExist(err) {
					return nil
				}
				require.NoError(t, err)

				trimmedPath := strings.TrimPrefix(path, targetHooksPath)
				if trimmedPath != "" {
					targetHooks = append(targetHooks, trimmedPath)
				}

				return nil
			}))
			require.ElementsMatch(t, setup.expectedCustomHooks, targetHooks)

			// Verify refs replicated.
			require.Equal(t,
				gittest.Exec(t, cfg, "-C", sourcePath, "show-ref", "--head"),
				gittest.Exec(t, cfg, "-C", targetPath, "show-ref", "--head"),
			)

			// Verify objects replicated.
			for _, oid := range setup.expectedObjects {
				gittest.Exec(t, cfg, "-C", targetPath, "cat-file", "-p", oid)
			}
		})
	}
}

func TestReplicateRepository_transactional(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalySSH(t, cfg)

	_, serverSocketPath := runRepositoryService(t, cfg, testserver.WithDisablePraefect())
	cfg.SocketPath = serverSocketPath

	sourceRepo, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	targetRepo := proto.Clone(sourceRepo).(*gitalypb.Repository)
	targetRepo.StorageName = cfg.Storages[1].Name

	var votes []string
	txServer := testTransactionServer{
		vote: func(request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
			votes = append(votes, hex.EncodeToString(request.ReferenceUpdatesHash))
			return &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_COMMIT,
			}, nil
		},
	}

	ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)
	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	client := newMuxedRepositoryClient(t, ctx, cfg, serverSocketPath, backchannel.NewClientHandshaker(
		testhelper.NewDiscardingLogEntry(t),
		func() backchannel.Server {
			srv := grpc.NewServer()
			gitalypb.RegisterRefTransactionServer(srv, &txServer)
			return srv
		},
		backchannel.DefaultConfiguration(),
	))

	// The first invocation creates the repository via a snapshot given that it doesn't yet
	// exist.
	_, err = client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     sourceRepo,
	})

	require.NoError(t, err)

	// There is no gitattributes file, so we vote on the empty contents of that file.
	gitattributesVote := sha1.Sum([]byte{})
	// There is a gitconfig though, so the vote should reflect its contents.
	gitconfigVote := sha1.Sum(testhelper.MustReadFile(t, filepath.Join(sourceRepoPath, "config")))

	noHooksVote := "fd69c38637bf443296edc19e2b2c649d0502f7c0"

	expectedVotes := []string{
		// We cannot easily derive these first two votes: they are based on the complete
		// hashed contents of the unpacked repository. We thus just only assert that they
		// are always the first two entries and that they are the same by simply taking the
		// first vote twice here.
		votes[0],
		votes[0],
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
		noHooksVote,
		noHooksVote,
	}

	require.Equal(t, expectedVotes, votes)

	// We're about to change refs/heads/master, and thus the mirror-fetch will update it. The
	// vote should reflect that.
	oldOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", sourceRepoPath, "rev-parse", "refs/heads/master"))
	newOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", sourceRepoPath, "rev-parse", "refs/heads/master~"))
	replicationVote := sha1.Sum([]byte(fmt.Sprintf("%[1]s %[2]s refs/heads/master\n%[1]s %[2]s HEAD\n", oldOID, newOID)))

	// We're now changing a reference in the source repository such that we can observe changes
	// in the target repo.
	gittest.Exec(t, cfg, "-C", sourceRepoPath, "update-ref", "refs/heads/master", "refs/heads/master~")

	votes = nil

	// And the second invocation uses FetchInternalRemote.
	_, err = client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     sourceRepo,
	})
	require.NoError(t, err)

	expectedVotes = []string{
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
		hex.EncodeToString(replicationVote[:]),
		hex.EncodeToString(replicationVote[:]),
		noHooksVote,
		noHooksVote,
	}

	require.Equal(t, expectedVotes, votes)
}

func TestFetchInternalRemote_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	remoteCfg := testcfg.Build(t)
	remoteRepo, remoteRepoPath := gittest.CreateRepository(t, ctx, remoteCfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})
	testcfg.BuildGitalyHooks(t, remoteCfg)
	gittest.WriteCommit(t, remoteCfg, remoteRepoPath, gittest.WithBranch("master"))

	_, remoteAddr := runRepositoryService(t, remoteCfg, testserver.WithDisablePraefect())

	localCfg := testcfg.Build(t)
	localRepoProto, localRepoPath := gittest.CreateRepository(t, ctx, localCfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})
	localRepo := localrepo.NewTestRepo(t, localCfg, localRepoProto)
	testcfg.BuildGitalySSH(t, localCfg)
	testcfg.BuildGitalyHooks(t, localCfg)
	gittest.Exec(t, remoteCfg, "-C", localRepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

	referenceTransactionHookCalled := 0

	// We do not require the server's address, but it needs to be around regardless such that
	// `FetchInternalRemote` can reach the hook service which is injected via the config.
	runRepositoryService(t, localCfg, testserver.WithHookManager(gitalyhook.NewMockManager(t, nil, nil, nil,
		func(t *testing.T, _ context.Context, _ gitalyhook.ReferenceTransactionState, _ []string, stdin io.Reader) error {
			// We need to discard stdin or otherwise the sending Goroutine may return an
			// EOF error and cause the test to fail.
			_, err := io.Copy(io.Discard, stdin)
			require.NoError(t, err)

			referenceTransactionHookCalled++
			return nil
		}),
	))

	ctx, err := storage.InjectGitalyServers(ctx, remoteRepo.GetStorageName(), remoteAddr, remoteCfg.Auth.Token)
	require.NoError(t, err)
	ctx = metadata.OutgoingToIncoming(ctx)

	getGitalySSHInvocationParams := listenGitalySSHCalls(t, localCfg)

	connsPool := client.NewPool()
	defer connsPool.Close()

	// Use the `assert` package such that we can get information about why hooks have failed via
	// the hook logs in case it did fail unexpectedly.
	assert.NoError(t, fetchInternalRemote(ctx, &transaction.MockManager{}, connsPool, localRepo, remoteRepo))

	hookLogs := filepath.Join(localCfg.Logging.Dir, "gitaly_hooks.log")
	require.FileExists(t, hookLogs)
	require.Equal(t, "", string(testhelper.MustReadFile(t, hookLogs)))

	require.Equal(t,
		string(gittest.Exec(t, remoteCfg, "-C", remoteRepoPath, "show-ref", "--head")),
		string(gittest.Exec(t, localCfg, "-C", localRepoPath, "show-ref", "--head")),
	)

	sshParams := getGitalySSHInvocationParams()
	require.Equal(t, []string{"upload-pack", "gitaly", "git-upload-pack", "'/internal.git'\n"}, sshParams.arguments)
	require.Subset(t,
		sshParams.environment,
		[]string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_VARIANT=simple",
			"LANG=en_US.UTF-8",
			"GITALY_ADDRESS=" + remoteAddr,
		},
	)

	require.Equal(t, 2, referenceTransactionHookCalled)
}

func TestFetchInternalRemote_failure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx = testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	connsPool := client.NewPool()
	defer connsPool.Close()

	err := fetchInternalRemote(ctx, &transaction.MockManager{}, connsPool, repo, &gitalypb.Repository{
		StorageName:  repoProto.GetStorageName(),
		RelativePath: "does-not-exist.git",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "exit status 128")
}

// gitalySSHParams contains parameters used to exec 'gitaly-ssh' binary.
type gitalySSHParams struct {
	arguments   []string
	environment []string
}

// listenGitalySSHCalls creates a script that intercepts 'gitaly-ssh' binary calls.
// It replaces 'gitaly-ssh' with a interceptor script that calls actual binary after flushing env var and
// arguments used for the binary invocation. That information will be returned back to the caller
// after invocation of the returned anonymous function.
func listenGitalySSHCalls(t *testing.T, conf config.Cfg) func() gitalySSHParams {
	t.Helper()

	require.NotEmpty(t, conf.BinDir)
	initialPath := conf.BinaryPath("gitaly-ssh")
	updatedPath := initialPath + "-actual"
	require.NoError(t, os.Rename(initialPath, updatedPath))

	tmpDir := testhelper.TempDir(t)

	script := fmt.Sprintf(`#!/usr/bin/env bash

		# To omit possible problem with parallel run and a race for the file creation with '>'
		# this option is used, please checkout https://mywiki.wooledge.org/NoClobber for more details.
		set -eo noclobber

		env >%[1]q/environment
		echo "$@" >%[1]q/arguments

		exec %[2]q "$@"`, tmpDir, updatedPath)
	require.NoError(t, os.WriteFile(initialPath, []byte(script), perm.SharedExecutable))

	return func() gitalySSHParams {
		arguments := testhelper.MustReadFile(t, filepath.Join(tmpDir, "arguments"))
		environment := testhelper.MustReadFile(t, filepath.Join(tmpDir, "environment"))
		return gitalySSHParams{
			arguments:   strings.Split(string(arguments), " "),
			environment: strings.Split(string(environment), "\n"),
		}
	}
}
