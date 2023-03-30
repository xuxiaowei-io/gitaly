//go:build !gitaly_test_sha256

package conflicts

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

type conflictFile struct {
	Header  *gitalypb.ConflictFileHeader
	Content []byte
}

func TestListConflictFiles(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	type setupData struct {
		request       *gitalypb.ListConflictFilesRequest
		client        gitalypb.ConflictsServiceClient
		expectedFiles []*conflictFile
		expectedError error
	}

	for _, tc := range []struct {
		desc  string
		setup func(testing.TB, context.Context) setupData
	}{
		{
			"Lists the expected conflict files",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("a"),
								OurPath:   []byte("a"),
								OurMode:   int32(0o100644),
							},
							Content: []byte("<<<<<<< a\napple\n=======\nmango\n>>>>>>> a\n"),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("b"),
								OurPath:   []byte("b"),
								OurMode:   int32(0o100644),
							},
							Content: []byte("<<<<<<< b\nbanana\n=======\npeach\n>>>>>>> b\n"),
						},
					},
				}
			},
		},
		{
			"Lists the expected conflict files with ancestor path",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				commonCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "grape"},
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "pineapple"},
					),
				)
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
					),
				)

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								TheirPath:    []byte("a"),
								OurPath:      []byte("a"),
								OurMode:      int32(0o100644),
								AncestorPath: []byte("a"),
							},
							Content: []byte("<<<<<<< a\ngrape\n=======\nmango\n>>>>>>> a\n"),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								TheirPath:    []byte("b"),
								OurPath:      []byte("b"),
								OurMode:      int32(0o100644),
								AncestorPath: []byte("b"),
							},
							Content: []byte("<<<<<<< b\npineapple\n=======\npeach\n>>>>>>> b\n"),
						},
					},
				}
			},
		},
		{
			"Lists the expected conflict files with huge diff",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: strings.Repeat("a\n", 128*1024)},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: strings.Repeat("b\n", 128*1024)},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: strings.Repeat("x\n", 128*1024)},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: strings.Repeat("y\n", 128*1024)},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("a"),
								OurPath:   []byte("a"),
								OurMode:   int32(0o100644),
							},
							Content: []byte(fmt.Sprintf("<<<<<<< a\n%s=======\n%s>>>>>>> a\n",
								strings.Repeat("a\n", 128*1024),
								strings.Repeat("x\n", 128*1024),
							)),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid: ourCommitID.String(),
								TheirPath: []byte("b"),
								OurPath:   []byte("b"),
								OurMode:   int32(0o100644),
							},
							Content: []byte(fmt.Sprintf("<<<<<<< b\n%s=======\n%s>>>>>>> b\n",
								strings.Repeat("b\n", 128*1024),
								strings.Repeat("y\n", 128*1024),
							)),
						},
					},
				}
			},
		},
		{
			"invalid commit id on 'our' side",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   "foobar",
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("could not lookup 'our' OID: reference not found"),
				}
			},
		},
		{
			"invalid commit id on 'their' side",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: "foobar",
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("could not lookup 'their' OID: reference not found"),
				}
			},
		},
		{
			"conflict side missing",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				commonCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					),
				)
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
					),
				)

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("conflict side missing"),
				}
			},
		},
		{
			"allow tree conflicts",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				commonCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
					gittest.TreeEntry{Path: "b", Mode: "100644", Content: "banana"},
				))
				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
					),
				)
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithParents(commonCommitID),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "b", Mode: "100644", Content: "peach"},
					),
				)

				request := &gitalypb.ListConflictFilesRequest{
					Repository:         repo,
					OurCommitOid:       ourCommitID.String(),
					TheirCommitOid:     theirCommitID.String(),
					AllowTreeConflicts: true,
				}

				return setupData{
					client:  client,
					request: request,
					expectedFiles: []*conflictFile{
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								OurPath:      []byte("a"),
								OurMode:      int32(0o100644),
								AncestorPath: []byte("a"),
							},
							Content: []byte("<<<<<<< a\nmango\n=======\n>>>>>>> \n"),
						},
						{
							Header: &gitalypb.ConflictFileHeader{
								CommitOid:    ourCommitID.String(),
								TheirPath:    []byte("b"),
								AncestorPath: []byte("b"),
							},
							Content: []byte("<<<<<<< \n=======\npeach\n>>>>>>> b\n"),
						},
					},
				}
			},
		},
		{
			"encoding error",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "a\xc5z"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "ascii normal"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewFailedPrecondition("unsupported encoding"),
				}
			},
		},
		{
			"empty repo",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				_, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
				))
				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     nil,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:  client,
					request: request,
					expectedError: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
						"empty Repository",
						"repo scoped: empty Repository",
					)),
				}
			},
		},
		{
			"empty OurCommitId field",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				theirCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "mango"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   "",
					TheirCommitOid: theirCommitID.String(),
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewInvalidArgument("empty OurCommitOid"),
				}
			},
		},
		{
			"empty TheirCommitId field",
			func(tb testing.TB, ctx context.Context) setupData {
				cfg, client := setupConflictsServiceWithoutRepo(tb, nil)
				repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

				ourCommitID := gittest.WriteCommit(tb, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "a", Mode: "100644", Content: "apple"},
				))

				request := &gitalypb.ListConflictFilesRequest{
					Repository:     repo,
					OurCommitOid:   ourCommitID.String(),
					TheirCommitOid: "",
				}

				return setupData{
					client:        client,
					request:       request,
					expectedError: structerr.NewInvalidArgument("empty TheirCommitOid"),
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			data := tc.setup(t, ctx)
			c, err := data.client.ListConflictFiles(ctx, data.request)
			if data.expectedError != nil && err == nil {
				err = drainListConflictFilesResponse(c)
			}
			testhelper.RequireGrpcError(t, data.expectedError, err)

			if data.expectedError == nil {
				testhelper.ProtoEqual(t, data.expectedFiles, getConflictFiles(t, c))
			}
		})
	}
}

func getConflictFiles(t *testing.T, c gitalypb.ConflictsService_ListConflictFilesClient) []*conflictFile {
	t.Helper()

	var files []*conflictFile
	var currentFile *conflictFile

	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for _, file := range r.GetFiles() {
			// If there's a header this is the beginning of a new file
			if header := file.GetHeader(); header != nil {
				if currentFile != nil {
					files = append(files, currentFile)
				}

				currentFile = &conflictFile{Header: header}
			} else {
				// Append to current file's content
				currentFile.Content = append(currentFile.Content, file.GetContent()...)
			}
		}
	}

	// Append leftover file
	files = append(files, currentFile)

	return files
}

func drainListConflictFilesResponse(c gitalypb.ConflictsService_ListConflictFilesClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
