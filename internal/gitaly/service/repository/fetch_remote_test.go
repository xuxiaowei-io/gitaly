//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestFetchRemote(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// Some of the tests require multiple calls to the clients each run struct
	// encompasses the expected data for a single run
	type run struct {
		expectedRefs     map[string]git.ObjectID
		expectedResponse *gitalypb.FetchRemoteResponse
		expectedErr      error
	}

	type setupData struct {
		repoPath string
		request  *gitalypb.FetchRemoteRequest
		runs     []run
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, cfg config.Cfg) setupData
	}{
		{
			desc: "check tags without tags",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						CheckTagsChanged: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/main": commitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{},
						},
					},
				}
			},
		},
		{
			desc: "check tags with tags",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))
				tagID := gittest.WriteTag(t, cfg, remoteRepoPath, "testtag", "main")

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						CheckTagsChanged: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/main":   commitID,
								"refs/tags/testtag": tagID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "check tags with tags (second pull)",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))
				tagID := gittest.WriteTag(t, cfg, remoteRepoPath, "testtag", "main")

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						CheckTagsChanged: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/main":   commitID,
								"refs/tags/testtag": tagID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/main":   commitID,
								"refs/tags/testtag": tagID,
							},
							// second time around it shouldn't have changed tags
							expectedResponse: &gitalypb.FetchRemoteResponse{},
						},
					},
				}
			},
		},
		{
			desc: "without checking for changed tags",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("main"))
				tagID := gittest.WriteTag(t, cfg, remoteRepoPath, "testtag", "main")

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						CheckTagsChanged: false,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/main":   commitID,
								"refs/tags/testtag": tagID,
							},
							// TagsChanged is set to true as we have requested to not check for tags changed
							// in the request so it defaults to true.
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
						// Run a second time to ensure it is consistent
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/main":   commitID,
								"refs/tags/testtag": tagID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "D/F conflict is resolved when pruning",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithReference("refs/heads/branch"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/heads/branch/conflict"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/branch": commitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
							expectedErr:      nil,
						},
					},
				}
			},
		},
		{
			desc: "D/F conflict causes failure when pruning is disabled",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithReference("refs/heads/branch"))
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/heads/branch/conflict"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						NoPrune: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/branch/conflict": commitID,
							},
							expectedErr: structerr.NewInternal(`fetch remote: "error: cannot lock ref 'refs/heads/branch': 'refs/heads/branch/conflict' exists; cannot create 'refs/heads/branch'\nerror: some local refs could not be updated; try running\n 'git remote prune inmemory' to remove any old, conflicting branches\n": exit status 1`),
						},
					},
				}
			},
		},
		{
			desc: "F/D conflict is resolved when pruning",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/heads/branch"))
				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithReference("refs/heads/branch/conflict"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/branch/conflict": commitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
							expectedErr:      nil,
						},
					},
				}
			},
		},
		{
			desc: "F/D conflict causes failure when pruning is disabled",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/heads/branch"))
				gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithReference("refs/heads/branch/conflict"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						NoPrune: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/branch": commitID,
							},
							expectedErr: structerr.NewInternal(`fetch remote: "error: cannot lock ref 'refs/heads/branch/conflict': 'refs/heads/branch' exists; cannot create 'refs/heads/branch/conflict'\nerror: some local refs could not be updated; try running\n 'git remote prune inmemory' to remove any old, conflicting branches\n": exit status 1`),
						},
					},
				}
			},
		},
		{
			desc: "with default refmaps",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Create the remote repository from which we're pulling from with two branches
				// that don't exist in the target repository.
				masterCommitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("master"), gittest.WithMessage("master"))
				featureCommitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("feature"), gittest.WithMessage("feature"))

				// Similar, we create the target repository with a branch that doesn't exist in the
				// source repository. This branch should get pruned by default.
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("unrelated"), gittest.WithMessage("unrelated"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/feature": featureCommitID,
								"refs/heads/master":  masterCommitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "NoPrune=true with explicit Remote should not delete reference",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				masterCommitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("master"))
				unrelatedCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("unrelated"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						NoPrune: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/unrelated": unrelatedCommitID,
								"refs/heads/master":    masterCommitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "NoPrune=false with explicit Remote should delete reference",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("master"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("unrelated"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						NoPrune: false,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/master": commitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "NoPrune=false with explicit Remote should not delete reference outside of refspec",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("master"))
				unrelatedID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("unrelated"))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url:           remoteRepoPath,
							MirrorRefmaps: []string{"refs/heads/*:refs/remotes/my-remote/*"},
						},
						NoPrune: false,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/remotes/my-remote/master": commitID,
								"refs/heads/unrelated":          unrelatedID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "remote params without force fails with diverging refs",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("master"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "foot", Content: "loose"}))
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "loose", Content: "foot"}))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{"refs/heads/master": commitID},
							expectedErr:  structerr.NewInternal("fetch remote: exit status 1"),
						},
					},
				}
			},
		},
		{
			desc: "remote params with force updates diverging refs",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch("master"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "foot", Content: "loose"}))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "loose", Content: "foot"}))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url: remoteRepoPath,
						},
						Force: true,
					},
					runs: []run{
						{
							expectedRefs:     map[string]git.ObjectID{"refs/heads/master": commitID},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "remote params with explicit refmap doesn't update divergent tag",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				remoteCommitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithReference("refs/tags/v1"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "foot", Content: "loose"}))
				gittest.WriteRef(t, cfg, remoteRepoPath, "refs/heads/master", remoteCommitID)
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "loose", Content: "foot"}))
				gittest.WriteRef(t, cfg, repoPath, "refs/heads/master", commitID)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url:           remoteRepoPath,
							MirrorRefmaps: []string{"+refs/heads/master:refs/heads/master"},
						},
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/master": remoteCommitID,
								"refs/tags/v1":      commitID,
							},
							expectedErr: structerr.NewInternal("fetch remote: exit status 1"),
						},
					},
				}
			},
		},
		{
			desc: "remote params with explicit refmap and force updates divergent tag",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				remoteCommitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithReference("refs/tags/v1"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "foot", Content: "loose"}))
				gittest.WriteRef(t, cfg, remoteRepoPath, "refs/heads/master", remoteCommitID)
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "loose", Content: "foot"}))
				gittest.WriteRef(t, cfg, repoPath, "refs/heads/master", commitID)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url:           remoteRepoPath,
							MirrorRefmaps: []string{"refs/heads/master:refs/heads/master"},
						},
						Force: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/master": remoteCommitID,
								"refs/tags/v1":      remoteCommitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "remote params with explicit refmap and no tags doesn't update divergent tag",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				remoteCommitID := gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithReference("refs/tags/v1"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "foot", Content: "loose"}))
				gittest.WriteRef(t, cfg, remoteRepoPath, "refs/heads/master", remoteCommitID)
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1"),
					gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "loose", Content: "foot"}))
				gittest.WriteRef(t, cfg, repoPath, "refs/heads/master", commitID)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url:           remoteRepoPath,
							MirrorRefmaps: []string{"+refs/heads/master:refs/heads/master"},
						},
						NoTags: true,
					},
					runs: []run{
						{
							expectedRefs: map[string]git.ObjectID{
								"refs/heads/master": remoteCommitID,
								"refs/tags/v1":      commitID,
							},
							expectedResponse: &gitalypb.FetchRemoteResponse{TagsChanged: true},
						},
					},
				}
			},
		},
		{
			desc: "no repository",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				_, repoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						RemoteParams: &gitalypb.Remote{Url: remoteRepoPath},
					},
					runs: []run{{expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
						"empty Repository",
						"repo scoped: empty Repository",
					))}},
				}
			},
		},
		{
			desc: "invalid storage",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: &gitalypb.Repository{
							StorageName:  "foobar",
							RelativePath: repoProto.RelativePath,
						},
						RemoteParams: &gitalypb.Remote{Url: remoteRepoPath},
					},
					runs: []run{{expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
						`fetch remote: GetStorageByName: no such storage: "foobar"`,
						"repo scoped: invalid Repository",
					))}},
				}
			},
		},
		{
			desc: "missing remote",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
					},
					runs: []run{{expectedErr: structerr.NewInvalidArgument("missing remote params")}},
				}
			},
		},
		{
			desc: "invalid remote URL",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository:   repoProto,
						RemoteParams: &gitalypb.Remote{Url: ""},
					},
					runs: []run{{expectedErr: structerr.NewInvalidArgument("blank or empty remote URL")}},
				}
			},
		},
		{
			desc: "/dev/null",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository:   repoProto,
						RemoteParams: &gitalypb.Remote{Url: "/dev/null"},
					},
					runs: []run{{expectedErr: structerr.NewInternal(`fetch remote: "fatal: '/dev/null' does not appear to be a git repository\nfatal: Could not read from remote repository.\n\nPlease make sure you have the correct access rights\nand the repository exists.\n": exit status 128`)}},
				}
			},
		},
		{
			desc: "non existent repo via http",
			setup: func(t *testing.T, cfg config.Cfg) setupData {
				_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gitCmdFactory := gittest.NewCommandFactory(t, cfg)
				port := gittest.HTTPServer(t, ctx, gitCmdFactory, remoteRepoPath, nil)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.FetchRemoteRequest{
						Repository: repoProto,
						RemoteParams: &gitalypb.Remote{
							Url:                     fmt.Sprintf("http://127.0.0.1:%d/%s", port, "invalid/repo/path.git"),
							HttpAuthorizationHeader: httpToken,
							HttpHost:                httpHost,
						},
					},
					runs: []run{
						{
							expectedErr: structerr.NewInternal(`fetch remote: "fatal: repository 'http://127.0.0.1:%d/invalid/repo/path.git/' not found\n": exit status 128`, port),
						},
					},
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg, client := setupRepositoryServiceWithoutRepo(t)
			setupData := tc.setup(t, cfg)

			for _, run := range setupData.runs {
				response, err := client.FetchRemote(ctx, setupData.request)
				testhelper.RequireGrpcError(t, run.expectedErr, err)
				testhelper.ProtoEqual(t, run.expectedResponse, response)

				var refs map[string]git.ObjectID
				refLines := text.ChompBytes(gittest.Exec(t, cfg, "-C", setupData.repoPath, "for-each-ref", `--format=%(refname) %(objectname)`))
				if refLines != "" {
					refs = make(map[string]git.ObjectID)
					for _, line := range strings.Split(refLines, "\n") {
						refname, objectID, found := strings.Cut(line, " ")
						require.True(t, found, "shouldn't have issues parsing the refs")
						refs[refname] = git.ObjectID(objectID)
					}
				}
				require.Equal(t, run.expectedRefs, refs)
			}
		})
	}
}

func TestFetchRemote_sshCommand(t *testing.T) {
	testhelper.SkipWithPraefect(t, "It's not possible to create repositories through the API with the git command overwritten by the script.")

	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})

	outputPath := filepath.Join(testhelper.TempDir(t), "output")

	// We ain't got a nice way to intercept the SSH call, so we just write a custom git command
	// which simply prints the GIT_SSH_COMMAND environment variable.
	gitCmdFactory := gittest.NewInterceptingCommandFactory(t, ctx, cfg, func(git.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/bin/sh
			for arg in $GIT_SSH_COMMAND
			do
				case "$arg" in
				-oIdentityFile=*)
					path=$(echo "$arg" | cut -d= -f2)
					cat "$path";;
				*)
					echo "$arg";;
				esac
			done >'%s'
			exit 7
		`, outputPath)
	})

	client, _ := runRepositoryService(t, cfg, nil, testserver.WithGitCommandFactory(gitCmdFactory))

	for _, tc := range []struct {
		desc           string
		request        *gitalypb.FetchRemoteRequest
		expectedOutput string
	}{
		{
			desc: "remote parameters without SSH key",
			request: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url: "https://example.com",
				},
			},
			expectedOutput: "ssh\n",
		},
		{
			desc: "remote parameters with SSH key",
			request: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url: "https://example.com",
				},
				SshKey: "mykey",
			},
			expectedOutput: "ssh\n-oIdentitiesOnly=yes\nmykey",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.FetchRemote(ctx, tc.request)
			require.Error(t, err)
			require.Contains(t, err.Error(), "fetch remote: exit status 7")

			output := testhelper.MustReadFile(t, outputPath)
			require.Equal(t, tc.expectedOutput, string(output))

			require.NoError(t, os.Remove(outputPath))
		})
	}
}

func TestFetchRemote_transaction(t *testing.T) {
	t.Parallel()
	sourceCfg := testcfg.Build(t)

	txManager := transaction.NewTrackingManager()
	client, addr := runRepositoryService(t, sourceCfg, nil, testserver.WithTransactionManager(txManager))
	sourceCfg.SocketPath = addr

	ctx := testhelper.Context(t)
	repo, _ := gittest.CreateRepository(t, ctx, sourceCfg, gittest.CreateRepositoryConfig{
		RelativePath: t.Name(),
		Seed:         gittest.SeedGitLabTest,
	})
	// Reset the manager as creating the repository casts some votes.
	txManager.Reset()

	targetCfg := testcfg.Build(t)
	targetRepoProto, targetRepoPath := gittest.CreateRepository(t, ctx, targetCfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
		RelativePath:           t.Name(),
	})
	targetGitCmdFactory := gittest.NewCommandFactory(t, targetCfg)

	port := gittest.HTTPServer(t, ctx, targetGitCmdFactory, targetRepoPath, nil)

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	require.Equal(t, 0, len(txManager.Votes()))

	_, err = client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
		Repository: targetRepoProto,
		RemoteParams: &gitalypb.Remote{
			Url: fmt.Sprintf("http://127.0.0.1:%d/%s", port, repo.GetRelativePath()),
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(txManager.Votes()))
}

const (
	httpToken = "ABCefg0999182"
	httpHost  = "example.com"
)

func remoteHTTPServer(t *testing.T, repoName, httpHost, httpToken string) (*httptest.Server, string) {
	b := testhelper.MustReadFile(t, "testdata/advertise.txt")

	s := httptest.NewServer(
		// https://github.com/git/git/blob/master/Documentation/technical/http-protocol.txt
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host != httpHost {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			if r.URL.String() != fmt.Sprintf("/%s.git/info/refs?service=git-upload-pack", repoName) {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			if httpToken != "" && r.Header.Get("Authorization") != httpToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			_, err := w.Write(b)
			assert.NoError(t, err)
		}),
	)

	return s, fmt.Sprintf("%s/%s.git", s.URL, repoName)
}

func getRefnames(t *testing.T, cfg config.Cfg, repoPath string) []string {
	result := gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref", "--format", "%(refname:lstrip=2)")
	return strings.Split(text.ChompBytes(result), "\n")
}

func TestFetchRemote_http(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		description string
		httpToken   string
		remoteURL   string
	}{
		{
			description: "with http token",
			httpToken:   httpToken,
		},
		{
			description: "without http token",
			httpToken:   "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			forkedRepo, forkedRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})

			s, remoteURL := remoteHTTPServer(t, "my-repo", httpHost, tc.httpToken)
			defer s.Close()

			req := &gitalypb.FetchRemoteRequest{
				Repository: forkedRepo,
				RemoteParams: &gitalypb.Remote{
					Url:                     remoteURL,
					HttpAuthorizationHeader: tc.httpToken,
					HttpHost:                httpHost,
					MirrorRefmaps:           []string{"all_refs"},
				},
				Timeout: 1000,
			}
			if tc.remoteURL != "" {
				req.RemoteParams.Url = s.URL + tc.remoteURL
			}

			refs := getRefnames(t, cfg, forkedRepoPath)
			require.True(t, len(refs) > 1, "the advertisement.txt should have deleted all refs except for master")

			_, err := client.FetchRemote(ctx, req)
			require.NoError(t, err)

			refs = getRefnames(t, cfg, forkedRepoPath)

			require.Len(t, refs, 1)
			assert.Equal(t, "master", refs[0])
		})
	}
}

func TestFetchRemote_localPath(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _, sourceRepoPath, client := setupRepositoryService(t, ctx)

	mirrorRepo, mirrorRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	_, err := client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
		Repository: mirrorRepo,
		RemoteParams: &gitalypb.Remote{
			Url: sourceRepoPath,
		},
	})
	require.NoError(t, err)

	require.Equal(t, getRefnames(t, cfg, sourceRepoPath), getRefnames(t, cfg, mirrorRepoPath))
}

func TestFetchRemote_httpWithRedirect(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(t, ctx)

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			http.Redirect(w, r, "/redirect_url", http.StatusSeeOther)
		}),
	)
	defer s.Close()

	req := &gitalypb.FetchRemoteRequest{
		Repository:   repo,
		RemoteParams: &gitalypb.Remote{Url: s.URL},
		Timeout:      1000,
	}

	_, err := client.FetchRemote(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "The requested URL returned error: 303")
}

func TestFetchRemote_httpWithTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(testhelper.Context(t))
	_, repo, _, client := setupRepositoryService(t, ctx)

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			// Block the request forever.
			<-ctx.Done()
		}),
	)
	defer func() {
		// We need to explicitly cancel the context here, or otherwise we'll be stuck
		// closing the server due to the ongoing request.
		cancel()
		s.Close()
	}()

	req := &gitalypb.FetchRemoteRequest{
		Repository:   repo,
		RemoteParams: &gitalypb.Remote{Url: s.URL},
		Timeout:      1,
	}

	_, err := client.FetchRemote(ctx, req)
	require.Error(t, err)

	require.Contains(t, err.Error(), "fetch remote: signal: terminated")
}

func TestFetchRemote_pooledRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// By default git-fetch(1) will always run with `core.alternateRefsCommand=exit 0 #`, which
	// effectively disables use of alternate refs. We can't just unset this value, so instead we
	// just write a script that knows to execute git-for-each-ref(1) as expected by this config
	// option.
	//
	// Note that we're using a separate command factory here just to ease the setup because we
	// need to recreate the other command factory with the Git configuration specified by the
	// test.
	alternateRefsCommandFactory := gittest.NewCommandFactory(t, testcfg.Build(t))
	exec := testhelper.WriteExecutable(t,
		filepath.Join(testhelper.TempDir(t), "alternate-refs"),
		[]byte(fmt.Sprintf(`#!/bin/sh
			exec %q -C "$1" for-each-ref --format='%%(objectname)'
		`, alternateRefsCommandFactory.GetExecutionEnvironment(ctx).BinaryPath)),
	)

	for _, tc := range []struct {
		desc                     string
		cfg                      config.Cfg
		shouldAnnouncePooledRefs bool
	}{
		{
			desc: "with default configuration",
		},
		{
			desc: "with alternates",
			cfg: config.Cfg{
				Git: config.Git{
					Config: []config.GitConfig{
						{
							Key:   "core.alternateRefsCommand",
							Value: exec,
						},
					},
				},
			},
			shouldAnnouncePooledRefs: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t, testcfg.WithBase(tc.cfg))
			gitCmdFactory := gittest.NewCommandFactory(t, cfg)

			client, swocketPath := runRepositoryService(t, cfg, nil, testserver.WithGitCommandFactory(gitCmdFactory))
			cfg.SocketPath = swocketPath

			// Create a repository that emulates an object pool. This object contains a
			// single reference with an object that is neither in the pool member nor in
			// the remote. If alternate refs are used, then Git will announce it to the
			// remote as "have".
			_, poolRepoPath := gittest.CreateRepository(t, ctx, cfg)
			poolCommitID := gittest.WriteCommit(t, cfg, poolRepoPath,
				gittest.WithBranch("pooled"),
				gittest.WithTreeEntries(gittest.TreeEntry{Path: "pool", Mode: "100644", Content: "pool contents"}),
			)

			// Create the pooled repository and link it to its pool. This is the
			// repository we're fetching into.
			pooledRepoProto, pooledRepoPath := gittest.CreateRepository(t, ctx, cfg)
			require.NoError(t, os.WriteFile(filepath.Join(pooledRepoPath, "objects", "info", "alternates"), []byte(filepath.Join(poolRepoPath, "objects")), 0o644))

			// And then finally create a third repository that emulates the remote side
			// we're fetching from. We need to create at least one reference so that Git
			// would actually try to fetch objects.
			_, remoteRepoPath := gittest.CreateRepository(t, ctx, cfg)
			gittest.WriteCommit(t, cfg, remoteRepoPath,
				gittest.WithBranch("remote"),
				gittest.WithTreeEntries(gittest.TreeEntry{Path: "remote", Mode: "100644", Content: "remote contents"}),
			)

			// Set up an HTTP server and intercept the request. This is done so that we
			// can observe the reference negotiation and check whether alternate refs
			// are announced or not.
			var requestBuffer bytes.Buffer
			port := gittest.HTTPServer(t, ctx, gitCmdFactory, remoteRepoPath, func(responseWriter http.ResponseWriter, request *http.Request, handler http.Handler) {
				closer := request.Body
				defer testhelper.MustClose(t, closer)

				request.Body = io.NopCloser(io.TeeReader(request.Body, &requestBuffer))

				handler.ServeHTTP(responseWriter, request)
			})

			// Perform the fetch.
			_, err := client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
				Repository: pooledRepoProto,
				RemoteParams: &gitalypb.Remote{
					Url: fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(remoteRepoPath)),
				},
			})
			require.NoError(t, err)

			// This should result in the "remote" branch having been fetched into the
			// pooled repository.
			require.Equal(t,
				gittest.ResolveRevision(t, cfg, pooledRepoPath, "refs/heads/remote"),
				gittest.ResolveRevision(t, cfg, remoteRepoPath, "refs/heads/remote"),
			)

			// Verify whether alternate refs have been announced as part of the
			// reference negotiation phase.
			if tc.shouldAnnouncePooledRefs {
				require.Contains(t, requestBuffer.String(), fmt.Sprintf("have %s", poolCommitID))
			} else {
				require.NotContains(t, requestBuffer.String(), fmt.Sprintf("have %s", poolCommitID))
			}
		})
	}
}
