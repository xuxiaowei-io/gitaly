//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc/status"
)

func TestCreateRepositoryFromBundle(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoClient := setupRepositoryServiceWithoutRepo(t)

	type setupData struct {
		repoProto    *gitalypb.Repository
		bundleData   []byte
		expectedRefs []git.Reference
		expectedErr  error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "create repository from bundle",
			setup: func(t *testing.T) setupData {
				_, bundleRepoPath := gittest.CreateRepository(t, ctx, cfg)

				// Create objects and references that will be used to validate the repository.
				oldRef := "refs/heads/old"
				newRef := "refs/heads/new"
				commitID1 := gittest.WriteCommit(t, cfg, bundleRepoPath, gittest.WithReference(oldRef))
				commitID2 := gittest.WriteCommit(t, cfg, bundleRepoPath, gittest.WithReference(newRef))

				// Change HEAD to validate the created repository will use the same reference.
				gittest.Exec(t, cfg, "-C", bundleRepoPath, "symbolic-ref", "HEAD", newRef)

				// Generate a Git bundle that will be used to create a repository from.
				bundleData := gittest.Exec(t, cfg, "-C", bundleRepoPath, "bundle", "create", "-", "--all")

				return setupData{
					repoProto: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: gittest.NewRepositoryName(t),
					},
					bundleData: bundleData,
					expectedRefs: []git.Reference{
						git.NewSymbolicReference("HEAD", git.ReferenceName(newRef)),
						git.NewReference(git.ReferenceName(oldRef), commitID1),
						git.NewReference(git.ReferenceName(newRef), commitID2),
					},
				}
			},
		},
		{
			desc: "invalid bundle",
			setup: func(t *testing.T) setupData {
				return setupData{
					// If an invalid Git bundle is transmitted, the RPC returns an error.
					repoProto: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: gittest.NewRepositoryName(t),
					},
					bundleData:  []byte("not-a-bundle"),
					expectedErr: structerr.NewInternal("fatal: invalid gitfile format:"),
				}
			},
		},
		{
			desc: "invalid argument",
			setup: func(t *testing.T) setupData {
				// If the repository is not specified in the RPC request, an error is returned.
				return setupData{
					repoProto:   nil,
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "repo already exists",
			setup: func(t *testing.T) setupData {
				// If the specified repository already exists a new one can not be created.
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repoProto: repoProto,
					expectedErr: testhelper.GitalyOrPraefect(
						structerr.NewAlreadyExists("creating repository: repository exists already"),
						structerr.NewAlreadyExists("route repository creation: reserve repository id: repository already exists"),
					),
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			stream, err := repoClient.CreateRepositoryFromBundle(ctx)
			require.NoError(t, err)

			writer := streamio.NewWriter(func(p []byte) error {
				if err := stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{Data: p}); err != nil {
					return err
				}
				return nil
			})

			// Some test cases will not transmit any bundle data. To ensure the first request with
			// repository information is received, explicitly send the first request.
			require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{Repository: setup.repoProto}))

			// If the test case is transmitting Git bundle data, write the content to the stream.
			_, err = writer.Write(setup.bundleData)
			require.NoError(t, err)

			_, err = stream.CloseAndRecv()

			// It is possible for the returned error to contain metadata embedded in its message
			// that makes it difficult to assert equivalency. For this reason, the status code is
			// verified, but the error message only asserts it contains a specified substring.
			if setup.expectedErr != nil {
				testhelper.RequireGrpcCode(t, err, status.Code(setup.expectedErr))
				require.ErrorContains(t, err, setup.expectedErr.Error())
				return
			}
			require.NoError(t, err)

			repo := localrepo.NewTestRepo(t, cfg, setup.repoProto)
			repoPath, err := repo.Path()
			require.NoError(t, err)

			// Verify connectivity and validity of the repository objects.
			gittest.Exec(t, cfg, "-C", repoPath, "fsck")

			refs, err := repo.GetReferences(ctx)
			require.NoError(t, err)

			headRef, err := repo.HeadReference(ctx)
			require.NoError(t, err)
			head := git.NewSymbolicReference("HEAD", headRef)

			// Verify repository contains references from the bundle.
			require.ElementsMatch(t, setup.expectedRefs, append(refs, head))
		})
	}
}

func TestCreateRepositoryFromBundle_transactional(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	txManager := transaction.NewTrackingManager()
	cfg, client := setupRepositoryServiceWithoutRepo(t, testserver.WithTransactionManager(txManager))

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// Reset the votes casted while creating the test repository.
	txManager.Reset()

	masterOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
	featureOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("feature"))

	// keep-around refs are not cloned in the initial step, but are added via the second call to
	// git-fetch(1). We thus create some of them to exercise their behaviour with regards to
	// transactional voting.
	for _, keepAroundRef := range []string{"refs/keep-around/1", "refs/keep-around/2"} {
		gittest.Exec(t, cfg, "-C", repoPath, "update-ref", keepAroundRef, masterOID.String())
	}

	ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	createdRepo := &gitalypb.Repository{
		StorageName:  repoProto.GetStorageName(),
		RelativePath: gittest.NewRepositoryName(t),
	}

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
		Repository: createdRepo,
	}))

	bundle := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", "-",
		"refs/heads/master", "refs/heads/feature", "refs/keep-around/1", "refs/keep-around/2")

	_, err = io.Copy(streamio.NewWriter(func(p []byte) error {
		require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
			Data: p,
		}))
		return nil
	}), bytes.NewReader(bundle))
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	createVote := func(hash string, phase voting.Phase) transaction.PhasedVote {
		vote, err := voting.VoteFromString(hash)
		require.NoError(t, err)
		return transaction.PhasedVote{Vote: vote, Phase: phase}
	}

	createdRepoPath, err := config.NewLocator(cfg).GetRepoPath(gittest.RewrittenRepository(t, ctx, cfg, createdRepo))
	require.NoError(t, err)

	refsVote := voting.VoteFromData([]byte(strings.Join([]string{
		fmt.Sprintf("%s %s refs/keep-around/2", gittest.DefaultObjectHash.ZeroOID, masterOID),
		fmt.Sprintf("%s %s refs/keep-around/1", gittest.DefaultObjectHash.ZeroOID, masterOID),
		fmt.Sprintf("%s %s refs/heads/feature", gittest.DefaultObjectHash.ZeroOID, featureOID),
		fmt.Sprintf("%s %s refs/heads/master", gittest.DefaultObjectHash.ZeroOID, masterOID),
	}, "\n") + "\n"))

	// Compute the second vote hash to assert that we really hash exactly the files that we
	// expect to hash. Furthermore, this is required for cross-platform compatibility given that
	// the configuration may be different depending on the platform.
	hash := voting.NewVoteHash()
	for _, filePath := range []string{
		"HEAD",
		"config",
		"refs/heads/feature",
		"refs/heads/master",
		"refs/keep-around/1",
		"refs/keep-around/2",
	} {
		file, err := os.Open(filepath.Join(createdRepoPath, filePath))
		require.NoError(t, err)

		_, err = io.Copy(hash, file)
		require.NoError(t, err)

		testhelper.MustClose(t, file)
	}

	filesVote, err := hash.Vote()
	require.NoError(t, err)

	require.Equal(t, []transaction.PhasedVote{
		// These are the votes created by git-fetch(1).
		createVote(refsVote.String(), voting.Prepared),
		createVote(refsVote.String(), voting.Committed),
		// And this is the manual votes we compute by walking the repository.
		createVote(filesVote.String(), voting.Prepared),
		createVote(filesVote.String(), voting.Committed),
	}, txManager.Votes())
}
