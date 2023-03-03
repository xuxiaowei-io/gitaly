//go:build !gitaly_test_sha256

package operations

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestUserUpdateSubmodule(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	type setupData struct {
		request          *gitalypb.UserUpdateSubmoduleRequest
		expectedResponse *gitalypb.UserUpdateSubmoduleResponse
		verify           func(t *testing.T)
		commitID         string
		expectedErr      error
	}

	testCases := []struct {
		desc    string
		subPath string
		branch  string
		setup   func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData
	}{
		{
			desc:    "successful",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}},
					commitID:         commitID.String(),
				}
			},
		},
		{
			desc:    "successful + weirdbranch",
			subPath: "sub",
			branch:  "refs/heads/master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("refs/heads/master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("refs/heads/master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}},
					commitID:         commitID.String(),
				}
			},
		},
		{
			desc:    "successful + nested folder",
			subPath: "foo/sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "foo/sub", subRepoPath),
					},
					gittest.TreeEntry{
						Mode: "040000",
						Path: "foo",
						OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{OID: subCommitID, Mode: "160000", Path: "sub"},
						}),
					},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("foo/sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}},
					commitID:         commitID.String(),
				}
			},
		},
		{
			desc:    "uses a quarantined repo",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				// Set up a hook that parses the new object and then aborts the update. Like this, we can
				// assert that the object does not end up in the main repository.
				outputPath := filepath.Join(testhelper.TempDir(t), "output")
				gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(
					`#!/bin/sh
					read oldval newval ref &&
					git rev-parse $newval^{commit} >%s &&
					exit 1
				`, outputPath)))

				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{
						PreReceiveError: fmt.Sprintf(`executing custom hooks: error executing "%s/custom_hooks/pre-receive": exit status 1`, repoPath),
					},
					commitID: commitID.String(),
					verify: func(t *testing.T) {
						hookOutput := testhelper.MustReadFile(t, outputPath)
						oid, err := gittest.DefaultObjectHash.FromHex(text.ChompBytes(hookOutput))
						require.NoError(t, err)

						repo := localrepo.NewTestRepo(t, cfg, repoProto)
						exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
						require.NoError(t, err)
						require.False(t, exists, "quarantined commit should have been discarded")
					},
				}
			},
		},
		{
			desc:    "failure due to empty repository",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect("empty Repository", "repo scoped: empty Repository")),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty user",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:    repoProto,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty User"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty submodule",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						Repository:    repoProto,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty Submodule"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty sha",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						Repository:    repoProto,
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty CommitSha"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to invalid sha",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						Repository:    repoProto,
						CommitSha:     "foobar",
						Branch:        []byte("master"),
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("invalid CommitSha"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty branch",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Repository:    repoProto,
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty Branch"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty commit message",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:       gittest.TestUser,
						CommitSha:  commitID.String(),
						Repository: repoProto,
						Branch:     []byte("master"),
						Submodule:  []byte("sub"),
					},
					expectedErr: structerr.NewInvalidArgument("empty CommitMessage"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to invalid branch",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("foobar"),
						Repository:    repoProto,
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedErr: structerr.NewInvalidArgument("Cannot find branch"),
					verify:      func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to invalid submodule",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     commitID.String(),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("foobar"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{
						CommitError: "Invalid submodule path",
					},
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to same submodule reference",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     subCommitID.String(),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("sub"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{
						CommitError: fmt.Sprintf("The submodule sub is already at %s", subCommitID),
					},
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to empty repository",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						User:          gittest.TestUser,
						CommitSha:     subCommitID.String(),
						Branch:        []byte("master"),
						Repository:    repoProto,
						Submodule:     []byte("foobar"),
						CommitMessage: []byte("Updating Submodule: sub"),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{
						CommitError: "Repository is empty",
					},
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "successful + expectedOldOID",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				expectedOldOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: expectedOldOID.String(),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}},
					commitID:         commitID.String(),
				}
			},
		},
		{
			desc:    "failure due to invalid expectedOldOID",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: "foobar",
					},
					commitID: commitID.String(),
					expectedErr: structerr.NewInvalidArgument(fmt.Sprintf(`invalid expected old object ID: invalid object ID: "foobar", expected length %v, got 6`, gittest.DefaultObjectHash.EncodedLen())).
						WithInterceptedMetadata("old_object_id", "foobar"),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to valid expectedOldOID SHA but not present in repo",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{
						Mode:    "100644",
						Path:    ".gitmodules",
						Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
					},
					gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
				))
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: gittest.DefaultObjectHash.ZeroOID.String(),
					},
					commitID: commitID.String(),
					expectedErr: structerr.NewInvalidArgument(`cannot resolve expected old object ID: reference not found`).
						WithInterceptedMetadata("old_object_id", gittest.DefaultObjectHash.ZeroOID.String()),
					verify: func(t *testing.T) {},
				}
			},
		},
		{
			desc:    "failure due to expectedOldOID pointing to an old commit",
			subPath: "sub",
			branch:  "master",
			setup: func(repoPath, subRepoPath string, repoProto, subRepoProto *gitalypb.Repository) setupData {
				subCommitID := gittest.WriteCommit(t, cfg, subRepoPath)
				firstCommit := gittest.WriteCommit(t, cfg, repoPath)
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"),
					gittest.WithParents(firstCommit),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    ".gitmodules",
							Content: fmt.Sprintf(`[submodule "%s"]\n\tpath = %s\n\turl = file://%s`, "sub", "sub", subRepoPath),
						},
						gittest.TreeEntry{OID: subCommitID, Mode: "160000", Path: "sub"},
					),
				)
				commitID := gittest.WriteCommit(t, cfg, subRepoPath, gittest.WithParents(subCommitID))

				return setupData{
					request: &gitalypb.UserUpdateSubmoduleRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitSha:      commitID.String(),
						Branch:         []byte("master"),
						Submodule:      []byte("sub"),
						CommitMessage:  []byte("Updating Submodule: sub"),
						ExpectedOldOid: firstCommit.String(),
					},
					expectedResponse: &gitalypb.UserUpdateSubmoduleResponse{
						CommitError: "Could not update refs/heads/master. Please refresh and try again.",
					},
					commitID: commitID.String(),
					verify:   func(t *testing.T) {},
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			subRepoProto, subRepoPath := gittest.CreateRepository(t, ctx, cfg)
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

			setupData := tc.setup(repoPath, subRepoPath, repoProto, subRepoProto)

			response, err := client.UserUpdateSubmodule(ctx, setupData.request)
			testhelper.RequireGrpcError(t, setupData.expectedErr, err)

			// If there is no verification function, lets do the default verification of
			// checking if the submodule was updated correctly in the main repo.
			if setupData.verify == nil {
				newCommitID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", string(setupData.request.Branch)))
				setupData.expectedResponse.BranchUpdate.CommitId = newCommitID

				entry := gittest.Exec(t, cfg, "-C", repoPath, "ls-tree", "-z", fmt.Sprintf("%s^{tree}:", response.BranchUpdate.CommitId), tc.subPath)
				parser := lstree.NewParser(bytes.NewReader(entry), git.ObjectHashSHA1)
				parsedEntry, err := parser.NextEntry()
				require.NoError(t, err)
				require.Equal(t, tc.subPath, parsedEntry.Path)
				require.Equal(t, setupData.commitID, parsedEntry.OID.String())
			} else {
				setupData.verify(t)
			}

			testhelper.ProtoEqual(t, setupData.expectedResponse, response)
		})
	}
}
