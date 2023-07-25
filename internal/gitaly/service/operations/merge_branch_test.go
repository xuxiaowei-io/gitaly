//go:build !gitaly_test_sha256

package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/signature"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb/testproto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	commitToMerge         = "e63f41fe459e62e1228fcef60d7189127aeba95a"
	mergeBranchName       = "gitaly-merge-test-branch"
	mergeBranchHeadBefore = "281d3a76f31c812dbf48abce82ccf6860adedd81"
)

//go:generate rm -rf testdata/gpg-keys testdata/signing_gpg_key testdata/signing_gpg_key.pub
//go:generate mkdir -p testdata/gpg-keys
//go:generate chmod 0700 testdata/gpg-keys
//go:generate gpg --homedir testdata/gpg-keys --generate-key --batch testdata/genkey.in
//go:generate gpg --homedir testdata/gpg-keys --export --output testdata/signing_gpg_key.pub
//go:generate gpg --homedir testdata/gpg-keys --export-secret-keys --output testdata/signing_gpg_key
func TestUserMergeBranch(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(t, testUserMergeBranch)
}

func testUserMergeBranch(t *testing.T, ctx context.Context) {
	var opts []testserver.GitalyServerOpt
	if featureflag.GPGSigning.IsEnabled(ctx) {
		opts = append(opts, testserver.WithSigningKey("testdata/signing_gpg_key"))
	}

	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx, opts...)

	if featureflag.GPGSigning.IsEnabled(ctx) {
		testcfg.BuildGitalyGPG(t, cfg)
	}

	type setupData struct {
		commitToMerge string
		masterCommit  string
		branch        string
		message       string
		repoPath      string
		repoProto     *gitalypb.Repository
	}

	// note we don't compare the main response, but rather the OperationBranchUpdate
	// this is mostly because the main response contains the commit ID for the merged
	// commit which we can't generate beforehand.
	type setupResponse struct {
		firstRequest           *gitalypb.UserMergeBranchRequest
		firstExpectedResponse  *gitalypb.OperationBranchUpdate
		firstExpectedErr       error
		secondRequest          *gitalypb.UserMergeBranchRequest
		secondExpectedResponse *gitalypb.OperationBranchUpdate
		secondExpectedErr      func(response *gitalypb.UserMergeBranchResponse) error
	}

	testCases := []struct {
		desc  string
		hooks []string
		setup func(data setupData) setupResponse
	}{
		{
			desc:  "merge successful",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository: data.repoProto,
						User:       gittest.TestUser,
						CommitId:   data.commitToMerge,
						Branch:     []byte(data.branch),
						Message:    []byte(data.message),
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
				}
			},
		},
		{
			desc:  "merge + hooks",
			hooks: GitlabHooks,
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository: data.repoProto,
						User:       gittest.TestUser,
						CommitId:   data.commitToMerge,
						Branch:     []byte(data.branch),
						Message:    []byte(data.message),
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
				}
			},
		},
		{
			desc:  "merge successful + expectedOldOID",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: data.masterCommit,
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
				}
			},
		},
		{
			desc:  "invalid expectedOldOID",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: "foobar",
					},
					firstExpectedErr: testhelper.WithInterceptedMetadata(
						structerr.NewInvalidArgument(fmt.Sprintf("invalid expected old object ID: invalid object ID: \"foobar\", expected length %v, got 6", gittest.DefaultObjectHash.EncodedLen())),
						"old_object_id", "foobar"),
				}
			},
		},
		{
			desc:  "expectedOldOID not present in repo",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: gittest.DefaultObjectHash.ZeroOID.String(),
					},
					firstExpectedErr: testhelper.WithInterceptedMetadata(
						structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found"),
						"old_object_id", gittest.DefaultObjectHash.ZeroOID),
				}
			},
		},
		{
			desc:  "incorrect expectedOldOID",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				secondCommit := gittest.WriteCommit(t, cfg, data.repoPath,
					gittest.WithParents(git.ObjectID(data.masterCommit)),
					gittest.WithBranch(data.branch),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
						gittest.TreeEntry{Mode: "100644", Path: "b", Content: "banana"},
					),
				)

				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: data.masterCommit,
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
					secondExpectedErr: func(response *gitalypb.UserMergeBranchResponse) error {
						return structerr.NewFailedPrecondition("reference update: reference does not point to expected object").
							WithDetail(&testproto.ErrorMetadata{
								Key:   []byte("actual_object_id"),
								Value: []byte(secondCommit),
							}).
							WithDetail(&testproto.ErrorMetadata{
								Key:   []byte("expected_object_id"),
								Value: []byte(data.masterCommit),
							}).
							WithDetail(&testproto.ErrorMetadata{
								Key:   []byte("reference"),
								Value: []byte("refs/heads/" + data.branch),
							}).
							WithDetail(&gitalypb.UserMergeBranchError{
								Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
									ReferenceUpdate: &gitalypb.ReferenceUpdateError{
										ReferenceName: []byte("refs/heads/" + data.branch),
										OldOid:        data.masterCommit,
										NewOid:        response.GetCommitId(),
									},
								},
							})
					},
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			branchToMerge := "master"
			message := "Merged by Gitaly"

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

			masterCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchToMerge),
				gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
				),
			)
			mergeCommitID := gittest.WriteCommit(t, cfg, repoPath,
				gittest.WithParents(masterCommitID),
				gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
					gittest.TreeEntry{Mode: "100644", Path: "foo", Content: "bar"},
				))

			data := tc.setup(setupData{
				commitToMerge: mergeCommitID.String(),
				masterCommit:  masterCommitID.String(),
				branch:        branchToMerge,
				message:       message,
				repoPath:      repoPath,
				repoProto:     repoProto,
			})

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			hookTempfiles := make([]string, len(tc.hooks))
			if len(tc.hooks) > 0 {
				tempDir := testhelper.TempDir(t)
				for i, hook := range tc.hooks {
					outputFile := filepath.Join(tempDir, hook)

					script := fmt.Sprintf("#!/bin/sh\n(cat && env) >%s \n", outputFile)
					gittest.WriteCustomHook(t, repoPath, hook, []byte(script))

					hookTempfiles[i] = outputFile
				}
			}

			require.NoError(t, mergeBidi.Send(data.firstRequest), "send first request")
			firstResponse, err := mergeBidi.Recv()

			if err != nil || data.firstExpectedErr != nil {
				testhelper.RequireGrpcError(t, data.firstExpectedErr, err)
				return
			}
			require.NoError(t, err, "receive first response")

			testhelper.ProtoEqual(t, data.firstExpectedResponse, firstResponse.BranchUpdate)

			if data.secondRequest == nil {
				return
			}

			if data.secondRequest.Apply && data.secondExpectedResponse != nil {
				data.secondExpectedResponse.CommitId = firstResponse.CommitId
			}

			require.NoError(t, mergeBidi.Send(data.secondRequest), "apply merge")
			secondResponse, err := mergeBidi.Recv()
			if data.secondExpectedErr != nil {
				if expectedErr := data.secondExpectedErr(firstResponse); err != nil || data.secondExpectedErr != nil {
					testhelper.RequireGrpcError(t, expectedErr, err)
					return
				}
			}
			require.NoError(t, err, "receive second response")

			testhelper.ProtoEqual(t, data.secondExpectedResponse, secondResponse.BranchUpdate)

			_, err = mergeBidi.Recv()
			require.Equal(t, io.EOF, err)

			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			commit, err := repo.ReadCommit(ctx, git.Revision(branchToMerge))
			require.NoError(t, err, "look up git commit after call has finished")

			require.Contains(t, commit.ParentIds, mergeCommitID.String())
			require.True(t, strings.HasPrefix(string(commit.Body), message), "expected %q to start with %q", commit.Body, message)

			if len(tc.hooks) > 0 {
				expectedGlID := "GL_ID=" + gittest.TestUser.GlId
				for i, h := range tc.hooks {
					hookEnv := testhelper.MustReadFile(t, hookTempfiles[i])

					lines := strings.Split(string(hookEnv), "\n")
					require.Contains(t, lines, expectedGlID, "expected env of hook %q to contain %q", h, expectedGlID)
					require.Contains(t, lines, "GL_PROTOCOL=web", "expected env of hook %q to contain GL_PROTOCOL")

					if h == "pre-receive" || h == "post-receive" {
						require.Regexp(t, masterCommitID.String()+" .* refs/heads/"+branchToMerge, lines[0], "expected env of hook %q to contain reference change", h)
					}
				}
			}

			if featureflag.GPGSigning.IsEnabled(ctx) {
				data, err := repo.ReadObject(ctx, git.ObjectID(branchToMerge))
				require.NoError(t, err)

				gpgsig, dataWithoutGpgSig := signature.ExtractSignature(t, ctx, data)

				pubKey := testhelper.MustReadFile(t, "testdata/signing_gpg_key.pub")
				keyring, err := openpgp.ReadKeyRing(bytes.NewReader(pubKey))
				require.NoError(t, err)

				_, err = openpgp.CheckArmoredDetachedSignature(
					keyring,
					strings.NewReader(dataWithoutGpgSig),
					strings.NewReader(gpgsig),
					&packet.Config{},
				)
				require.NoError(t, err)
			}
		})
	}
}

func TestUserMergeBranch_failure(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchFailure,
	)
}

func testUserMergeBranchFailure(t *testing.T, ctx context.Context) {
	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	master := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
		gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
	))
	commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Mode: "100644", Path: "b", Content: "banana"},
	))
	branchToMerge := "branchToMerge"
	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(branchToMerge),
		gittest.WithParents(commit1),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "b", Content: "banana"},
		),
	)

	testCases := []struct {
		user             *gitalypb.User
		repo             *gitalypb.Repository
		desc             string
		commitID         string
		expectedOldOid   string
		branch           []byte
		message          []byte
		setup            func() *gitalypb.UserMergeBranchRequest
		expectedErr      error
		expectedApplyErr string
	}{
		{
			desc: "no repository provided",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					User: gittest.TestUser,
				}
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty user",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					CommitId:   master.Revision().String(),
					Branch:     []byte(branchToMerge),
					Message:    []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty user"),
		},
		{
			desc: "empty user name",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User: &gitalypb.User{
						GlId:       gittest.TestUser.GlId,
						GlUsername: gittest.TestUser.GlUsername,
						Email:      gittest.TestUser.Email,
						Timezone:   gittest.TestUser.Timezone,
					},
					CommitId: master.Revision().String(),
					Branch:   []byte(branchToMerge),
					Message:  []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty user name"),
		},
		{
			desc: "empty user email",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User: &gitalypb.User{
						GlId:       gittest.TestUser.GlId,
						GlUsername: gittest.TestUser.GlUsername,
						Name:       gittest.TestUser.Name,
						Timezone:   gittest.TestUser.Timezone,
					},
					CommitId: master.Revision().String(),
					Branch:   []byte(branchToMerge),
					Message:  []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty user email"),
		},
		{
			desc: "empty commit",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User:       gittest.TestUser,
					Branch:     []byte(branchToMerge),
					Message:    []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty commit ID"),
		},
		{
			desc: "empty branch",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User:       gittest.TestUser,
					CommitId:   master.Revision().String(),
					Message:    []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty branch name"),
		},
		{
			desc: "empty message",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User:       gittest.TestUser,
					CommitId:   master.Revision().String(),
					Branch:     []byte(branchToMerge),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty message"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := tc.setup()
			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			require.NoError(t, mergeBidi.Send(request), "apply merge")
			_, err = mergeBidi.Recv()

			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err, "receive first response")
			require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
			_, err = mergeBidi.Recv()

			require.EqualError(t, err, tc.expectedApplyErr)
		})
	}
}

func TestUserMergeBranch_quarantine(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchQuarantine,
	)
}

func testUserMergeBranchQuarantine(t *testing.T, ctx context.Context) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// Set up a hook that parses the merge commit and then aborts the update. Like this, we
	// can assert that the object does not end up in the main repository.
	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(
		`#!/bin/sh
		read oldval newval ref &&
		git rev-parse $newval^{commit} >&2 &&
		git rev-parse $oldval^{commit} &&
		exit 1
	`))

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	stream, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("Merged by Gitaly"),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
	}))

	firstResponse, err := stream.Recv()
	require.NoError(t, err, "receive first response")

	require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	secondResponse, err := stream.Recv()
	testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("%s\n", firstResponse.CommitId).WithDetail(
		&gitalypb.UserMergeBranchError{
			Error: &gitalypb.UserMergeBranchError_CustomHook{
				CustomHook: &gitalypb.CustomHookError{
					HookType: gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
					Stdout:   []byte(fmt.Sprintf("%s\n", mergeBranchHeadBefore)),
					Stderr:   []byte(fmt.Sprintf("%s\n", firstResponse.CommitId)),
				},
			},
		},
	), err)
	require.Nil(t, secondResponse)

	oid, err := git.ObjectHashSHA1.FromHex(strings.TrimSpace(firstResponse.CommitId))
	require.NoError(t, err)
	exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
	require.NoError(t, err)

	require.False(t, exists, "quarantined commit should have been discarded")
}

func TestUserMergeBranch_stableMergeIDs(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchStableMergeIDs,
	)
}

func testUserMergeBranchStableMergeIDs(t *testing.T, ctx context.Context) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("Merged by Gitaly"),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
	}

	// Because the timestamp is
	expectedMergeID := "f0165798887392f9148b55d54a832b005f93a38c"

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	response, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")
	require.Equal(t, response.CommitId, expectedMergeID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	response, err = mergeBidi.Recv()
	require.NoError(t, err, "receive second response")
	require.Equal(t, expectedMergeID, response.BranchUpdate.CommitId)

	_, err = mergeBidi.Recv()
	require.Equal(t, io.EOF, err)

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "look up git commit after call has finished")
	require.Equal(t, commit, &gitalypb.GitCommit{
		Subject:  []byte("Merged by Gitaly"),
		Body:     []byte("Merged by Gitaly"),
		BodySize: 16,
		Id:       expectedMergeID,
		ParentIds: []string{
			"281d3a76f31c812dbf48abce82ccf6860adedd81",
			"e63f41fe459e62e1228fcef60d7189127aeba95a",
		},
		TreeId: "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
		Author: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamppb.Timestamp{Seconds: 12},
			Timezone: []byte(gittest.TimezoneOffset),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamppb.Timestamp{Seconds: 12},
			Timezone: []byte(gittest.TimezoneOffset),
		},
	})
}

func TestUserMergeBranch_abort(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchAbort,
	)
}

func testUserMergeBranchAbort(t *testing.T, ctx context.Context) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("foobar"),
	}

	testCases := []struct {
		req       *gitalypb.UserMergeBranchRequest
		closeSend bool
		desc      string
	}{
		{req: &gitalypb.UserMergeBranchRequest{Repository: &gitalypb.Repository{}}, desc: "empty request, don't close"},
		{req: &gitalypb.UserMergeBranchRequest{Repository: &gitalypb.Repository{}}, closeSend: true, desc: "empty request and close"},
		{closeSend: true, desc: "no request just close"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

			firstResponse, err := mergeBidi.Recv()
			require.NoError(t, err, "first response")
			require.NotEqual(t, "", firstResponse.CommitId, "commit ID on first response")

			if tc.req != nil {
				require.NoError(t, mergeBidi.Send(tc.req), "send second request")
			}

			if tc.closeSend {
				require.NoError(t, mergeBidi.CloseSend(), "close request stream from client")
			}

			secondResponse, err := mergeBidi.Recv()
			require.Equal(t, "", secondResponse.GetBranchUpdate().GetCommitId(), "merge should not have been applied")
			require.Error(t, err)

			commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
			require.NoError(t, err, "look up git commit after call has finished")

			require.Equal(t, mergeBranchHeadBefore, commit.Id, "branch should not change when the merge is aborted")
		})
	}
}

func TestUserMergeBranch_concurrentUpdate(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchConcurrentUpdate,
	)
}

func testUserMergeBranchConcurrentUpdate(t *testing.T, ctx context.Context) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	// This concurrent update of the branch we are merging into should make the merge fail.
	concurrentCommitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(mergeBranchName))
	require.NotEqual(t, firstResponse.CommitId, concurrentCommitID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	require.NoError(t, mergeBidi.CloseSend(), "close send")

	secondResponse, err := mergeBidi.Recv()
	testhelper.RequireGrpcError(t,
		structerr.NewFailedPrecondition("reference update: reference does not point to expected object").
			WithDetail(&testproto.ErrorMetadata{
				Key:   []byte("actual_object_id"),
				Value: []byte(concurrentCommitID),
			}).
			WithDetail(&testproto.ErrorMetadata{
				Key:   []byte("expected_object_id"),
				Value: []byte("281d3a76f31c812dbf48abce82ccf6860adedd81"),
			}).
			WithDetail(&testproto.ErrorMetadata{
				Key:   []byte("reference"),
				Value: []byte("refs/heads/" + mergeBranchName),
			}).
			WithDetail(&gitalypb.UserMergeBranchError{
				Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
					ReferenceUpdate: &gitalypb.ReferenceUpdateError{
						ReferenceName: []byte("refs/heads/" + mergeBranchName),
						OldOid:        "281d3a76f31c812dbf48abce82ccf6860adedd81",
						NewOid:        "f0165798887392f9148b55d54a832b005f93a38c",
					},
				},
			}),
		err,
	)
	require.Nil(t, secondResponse)

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "get commit after RPC finished")
	require.Equal(t, commit.Id, concurrentCommitID.String(), "RPC should not have trampled concurrent update")
}

func TestUserMergeBranch_ambiguousReference(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchAmbiguousReference,
	)
}

func testUserMergeBranchAmbiguousReference(t *testing.T, ctx context.Context) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	merge, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	masterOID, err := repo.ResolveRevision(ctx, "refs/heads/master")
	require.NoError(t, err)

	// We're now creating all kinds of potentially ambiguous references in
	// the hope that UserMergeBranch won't be confused by it.
	for _, reference := range []string{
		mergeBranchName,
		"heads/" + mergeBranchName,
		"refs/heads/refs/heads/" + mergeBranchName,
		"refs/tags/" + mergeBranchName,
		"refs/tags/heads/" + mergeBranchName,
		"refs/tags/refs/heads/" + mergeBranchName,
	} {
		require.NoError(t, repo.UpdateRef(ctx, git.ReferenceName(reference), masterOID, git.ObjectHashSHA1.ZeroOID))
	}

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, merge.Send(firstRequest), "send first request")

	_, err = merge.Recv()
	require.NoError(t, err, "receive first response")
	require.NoError(t, err, "look up git commit before merge is applied")
	require.NoError(t, merge.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")

	response, err := merge.Recv()
	require.NoError(t, err, "receive second response")

	_, err = merge.Recv()
	require.Equal(t, io.EOF, err)

	commit, err := repo.ReadCommit(ctx, git.Revision("refs/heads/"+mergeBranchName))
	require.NoError(t, err, "look up git commit after call has finished")

	testhelper.ProtoEqual(t, &gitalypb.OperationBranchUpdate{CommitId: commit.Id}, response.BranchUpdate)
	require.Equal(t, mergeCommitMessage, string(commit.Body))
	require.Equal(t, gittest.TestUser.Name, commit.Author.Name)
	require.Equal(t, gittest.TestUser.Email, commit.Author.Email)
	require.Equal(t, []string{mergeBranchHeadBefore, commitToMerge}, commit.ParentIds)
}

func TestUserMergeBranch_failingHooks(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchFailingHooks,
	)
}

func testUserMergeBranchFailingHooks(t *testing.T, ctx context.Context) {
	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	hookContent := []byte("#!/bin/sh\necho 'stdout' && echo 'stderr' >&2\nexit 1")

	for _, tc := range []struct {
		hookName   string
		hookType   gitalypb.CustomHookError_HookType
		shouldFail bool
	}{
		{
			hookName:   "pre-receive",
			hookType:   gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
			shouldFail: true,
		},
		{
			hookName:   "update",
			hookType:   gitalypb.CustomHookError_HOOK_TYPE_UPDATE,
			shouldFail: true,
		},
		{
			hookName: "post-receive",
			hookType: gitalypb.CustomHookError_HOOK_TYPE_POSTRECEIVE,
			// The post-receive hook runs after references have been updated and any
			// failures of it are ignored.
			shouldFail: false,
		},
	} {
		t.Run(tc.hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, tc.hookName, hookContent)

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			mergeCommitMessage := "Merged by Gitaly"
			firstRequest := &gitalypb.UserMergeBranchRequest{
				Repository: repo,
				User:       gittest.TestUser,
				CommitId:   commitToMerge,
				Branch:     []byte(mergeBranchName),
				Message:    []byte(mergeCommitMessage),
			}

			require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

			firstResponse, err := mergeBidi.Recv()
			require.NoError(t, err, "receive first response")

			require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
			require.NoError(t, mergeBidi.CloseSend(), "close send")

			secondResponse, err := mergeBidi.Recv()
			if tc.shouldFail {
				testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("stderr\n").WithDetail(
					&gitalypb.UserMergeBranchError{
						Error: &gitalypb.UserMergeBranchError_CustomHook{
							CustomHook: &gitalypb.CustomHookError{
								HookType: tc.hookType,
								Stdout:   []byte("stdout\n"),
								Stderr:   []byte("stderr\n"),
							},
						},
					},
				), err)
				require.Nil(t, secondResponse)
			} else {
				testhelper.ProtoEqual(t, &gitalypb.UserMergeBranchResponse{
					BranchUpdate: &gitalypb.OperationBranchUpdate{
						CommitId: firstResponse.CommitId,
					},
				}, secondResponse)
				require.NoError(t, err)

				response, err := mergeBidi.Recv()
				require.Equal(t, io.EOF, err)
				require.Nil(t, response)
			}

			currentBranchHead := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", mergeBranchName)
			if !tc.shouldFail {
				require.Equal(t, firstResponse.CommitId, text.ChompBytes(currentBranchHead), "branch head updated")
			} else {
				require.Equal(t, mergeBranchHeadBefore, text.ChompBytes(currentBranchHead), "branch head updated")
			}
		})
	}
}

func TestUserMergeBranch_conflict(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchConflict,
	)
}

func testUserMergeBranchConflict(t *testing.T, ctx context.Context) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	const mergeIntoBranch = "mergeIntoBranch"
	const mergeFromBranch = "mergeFromBranch"
	const conflictingFile = "file"

	baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(mergeIntoBranch), gittest.WithTreeEntries(gittest.TreeEntry{
		Mode: "100644", Path: conflictingFile, Content: "data",
	}))

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeFromBranch, baseCommit.String())

	divergedInto := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(mergeIntoBranch), gittest.WithTreeEntries(gittest.TreeEntry{
		Mode: "100644", Path: conflictingFile, Content: "data-1",
	}))

	divergedFrom := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(mergeFromBranch), gittest.WithTreeEntries(gittest.TreeEntry{
		Mode: "100644", Path: conflictingFile, Content: "data-2",
	}))

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Branch:     []byte(mergeIntoBranch),
		CommitId:   divergedFrom.String(),
		Message:    []byte("msg"),
	}), "send first request")

	firstResponse, err := mergeBidi.Recv()
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("merging commits: merge: there are conflicting files").WithDetail(
		&gitalypb.UserMergeBranchError{
			Error: &gitalypb.UserMergeBranchError_MergeConflict{
				MergeConflict: &gitalypb.MergeConflictError{
					ConflictingFiles: [][]byte{
						[]byte(conflictingFile),
					},
					ConflictingCommitIds: []string{
						divergedInto.String(),
						divergedFrom.String(),
					},
				},
			},
		},
	), err)
	require.Nil(t, firstResponse)
}

func TestUserMergeBranch_allowed(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(
		t,
		testUserMergeBranchAllowed,
	)
}

func testUserMergeBranchAllowed(t *testing.T, ctx context.Context) {
	mergeBranchHeadAfter := "ff0ac4dfa30d6b26fd14aa83a75650355270bf76"

	for _, tc := range []struct {
		desc             string
		allowed          bool
		allowedMessage   string
		allowedErr       error
		expectedErr      error
		expectedResponse *gitalypb.UserMergeBranchResponse
	}{
		{
			desc:    "allowed",
			allowed: true,
			expectedResponse: &gitalypb.UserMergeBranchResponse{
				BranchUpdate: &gitalypb.OperationBranchUpdate{
					CommitId: mergeBranchHeadAfter,
				},
			},
		},
		{
			desc:           "disallowed",
			allowed:        false,
			allowedMessage: "you shall not pass",
			expectedErr: structerr.NewPermissionDenied("GitLab: you shall not pass").WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: "you shall not pass",
							Protocol:     "web",
							UserId:       gittest.GlID,
							Changes:      []byte(fmt.Sprintf("%s %s refs/heads/%s\n", mergeBranchHeadBefore, mergeBranchHeadAfter, mergeBranchName)),
						},
					},
				},
			),
		},
		{
			desc:       "failing",
			allowedErr: errors.New("failure"),
			expectedErr: structerr.NewPermissionDenied("GitLab: failure").WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: "failure",
							Protocol:     "web",
							UserId:       gittest.GlID,
							Changes:      []byte(fmt.Sprintf("%s %s refs/heads/%s\n", mergeBranchHeadBefore, mergeBranchHeadAfter, mergeBranchName)),
						},
					},
				},
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)
			backchannelRegistry := backchannel.NewRegistry()
			txManager := transaction.NewManager(cfg, backchannelRegistry)
			hookManager := hook.NewManager(cfg, config.NewLocator(cfg), gittest.NewCommandFactory(t, cfg), txManager, gitlab.NewMockClient(
				t,
				func(context.Context, gitlab.AllowedParams) (bool, string, error) {
					return tc.allowed, tc.allowedMessage, tc.allowedErr
				},
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
			))

			ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithCfg(
				t, ctx, cfg,
				testserver.WithBackchannelRegistry(backchannelRegistry),
				testserver.WithTransactionManager(txManager),
				testserver.WithHookManager(hookManager),
			)

			gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

			stream, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				CommitId:   commitToMerge,
				Branch:     []byte(mergeBranchName),
				Message:    []byte("message"),
				Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
			}))

			response, err := stream.Recv()
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.UserMergeBranchResponse{
				CommitId: mergeBranchHeadAfter,
			}, response)

			require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{
				Apply: true,
			}))

			response, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)

			if err == nil {
				_, err = stream.Recv()
				require.Equal(t, io.EOF, err)
			}
		})
	}
}
