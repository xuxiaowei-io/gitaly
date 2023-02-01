//go:build !gitaly_test_sha256

package operations

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var commitFilesMessage = []byte("Change files")

func TestUserCommitFiles(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testUserCommitFiles)
}

func testUserCommitFiles(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	const (
		DefaultMode    = "100644"
		ExecutableMode = "100755"
	)

	startRepo, _ := gittest.CreateRepository(t, ctx, cfg)
	targetRepoSentinel := &gitalypb.Repository{}

	type step struct {
		actions           []*gitalypb.UserCommitFilesRequest
		startRepository   *gitalypb.Repository
		startBranch       string
		error             error
		indexError        string
		structError       *git2go.IndexError
		unknownIndexError error
		repoCreated       bool
		branchCreated     bool
		treeEntries       []gittest.TreeEntry
	}

	for _, tc := range []struct {
		desc  string
		steps []step
	}{
		{
			desc: "create file with .git/hooks/pre-commit",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest(".git/hooks/pre-commit"),
						actionContentRequest("content-1"),
					},
					indexError:        "invalid path: '.git/hooks/pre-commit'",
					unknownIndexError: status.Error(codes.Internal, "invalid path: '.git/hooks/pre-commit'"),
				},
			},
		},
		{
			desc: "create directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory ignores mode and content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
									FilePath:        []byte("directory-1"),
									ExecuteFilemode: true,
									Base64Content:   true,
								},
							},
						}),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory created duplicate",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
						createDirHeaderRequest("directory-1"),
					},
					indexError:  "A directory with this name already exists",
					structError: &git2go.IndexError{Path: "directory-1", Type: git2go.ErrDirectoryExists},
				},
			},
		},
		{
			desc: "create directory with traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("../directory-1"),
					},
					indexError:  "Path cannot include directory traversal",
					structError: &git2go.IndexError{Path: "../directory-1", Type: git2go.ErrDirectoryTraversal},
				},
			},
		},
		{
			desc: "create directory existing duplicate",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					indexError:  "A directory with this name already exists",
					structError: &git2go.IndexError{Path: "directory-1", Type: git2go.ErrDirectoryExists},
				},
			},
		},
		{
			desc: "create directory with files name",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("file-1"),
					},
					indexError:  "A file with this name already exists",
					structError: &git2go.IndexError{Path: "file-1", Type: git2go.ErrFileExists},
				},
			},
		},
		{
			desc: "create file with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("../file-1"),
						actionContentRequest("content-1"),
					},
					indexError:  "Path cannot include directory traversal",
					structError: &git2go.IndexError{Path: "../file-1", Type: git2go.ErrDirectoryTraversal},
				},
			},
		},
		{
			desc: "create file with double slash",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("invalid://file/name/here"),
						actionContentRequest("content-1"),
					},
					indexError:  "invalid path: 'invalid://file/name/here'",
					structError: &git2go.IndexError{Path: "invalid://file/name/here", Type: git2go.ErrInvalidPath},
				},
			},
		},
		{
			desc: "create file without content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "create file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						actionContentRequest(" content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1 content-2"},
					},
				},
			},
		},
		{
			desc: "create file with unclean path",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("/file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "create file with base64 content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createBase64FileHeaderRequest("file-1"),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte("content-1"))),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte(" content-2"))),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1 content-2"},
					},
				},
			},
		},
		{
			desc: "create file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1\r\n"),
						actionContentRequest(" content-2\r\n"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1\n content-2\n"},
					},
				},
			},
		},
		{
			desc: "create duplicate file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						createFileHeaderRequest("file-1"),
					},
					indexError:  "A file with this name already exists",
					structError: &git2go.IndexError{Path: "file-1", Type: git2go.ErrFileExists},
				},
			},
		},
		{
			desc: "create file overwrites directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("file-1"),
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "update created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2\r\n"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2\n"},
					},
				},
			},
		},
		{
			desc: "update base64 content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateBase64FileHeaderRequest("file-1"),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte("content-2"))),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update ignores mode",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_UPDATE,
									FilePath:        []byte("file-1"),
									ExecuteFilemode: true,
								},
							},
						}),
						actionContentRequest("content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2"),
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						updateFileHeaderRequest("non-existing"),
						actionContentRequest("content"),
					},
					indexError:  "A file with this name doesn't exist",
					structError: &git2go.IndexError{Path: "non-existing", Type: git2go.ErrFileNotFound},
				},
			},
		},
		{
			desc: "move file with traversal in source",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("../original-file", "moved-file", true),
					},
					indexError:  "Path cannot include directory traversal",
					structError: &git2go.IndexError{Path: "../original-file", Type: git2go.ErrDirectoryTraversal},
				},
			},
		},
		{
			desc: "move file with traversal in destination",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "../moved-file", true),
					},
					indexError:  "Path cannot include directory traversal",
					structError: &git2go.IndexError{Path: "../moved-file", Type: git2go.ErrDirectoryTraversal},
				},
			},
		},
		{
			desc: "move created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("content-1"),
						moveFileHeaderRequest("original-file", "moved-file", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "move ignores mode",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("content-1"),
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_MOVE,
									FilePath:        []byte("moved-file"),
									PreviousPath:    []byte("original-file"),
									ExecuteFilemode: true,
									InferContent:    true,
								},
							},
						}),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "moving directory fails",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory"),
						moveFileHeaderRequest("directory", "moved-directory", true),
					},
					indexError:  "A file with this name doesn't exist",
					structError: &git2go.IndexError{Path: "directory", Type: git2go.ErrFileNotFound},
				},
			},
		},
		{
			desc: "move file inferring content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("original-content"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original-content"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "moved-file", true),
						actionContentRequest("ignored-content"),
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original-content"},
					},
				},
			},
		},
		{
			desc: "move file with non-existing source",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("non-existing", "destination-file", true),
					},
					indexError:  "A file with this name doesn't exist",
					structError: &git2go.IndexError{Path: "non-existing", Type: git2go.ErrFileNotFound},
				},
			},
		},
		{
			desc: "move file with already existing destination file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("source-file"),
						createFileHeaderRequest("already-existing"),
						moveFileHeaderRequest("source-file", "already-existing", true),
					},
					indexError:  "A file with this name already exists",
					structError: &git2go.IndexError{Path: "already-existing", Type: git2go.ErrFileExists},
				},
			},
		},
		{
			// seems like a bug in the original implementation to allow overwriting a
			// directory
			desc: "move file with already existing destination directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("source-file"),
						actionContentRequest("source-content"),
						createDirHeaderRequest("already-existing"),
						moveFileHeaderRequest("source-file", "already-existing", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "already-existing", Content: "source-content"},
					},
				},
			},
		},
		{
			desc: "move file providing content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("original-content"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original-content"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "moved-file", false),
						actionContentRequest("new-content"),
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "new-content"},
					},
				},
			},
		},
		{
			desc: "move file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("original-content"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original-content"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "moved-file", false),
						actionContentRequest("new-content\r\n"),
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "new-content\n"},
					},
				},
			},
		},
		{
			desc: "mark non-existing file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					indexError:  "A file with this name doesn't exist",
					structError: &git2go.IndexError{Path: "file-1", Type: git2go.ErrFileNotFound},
				},
			},
		},
		{
			desc: "mark executable file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						chmodFileHeaderRequest("file-1", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "mark file executable with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("../file-1", true),
					},
					indexError:  "Path cannot include directory traversal",
					structError: &git2go.IndexError{Path: "../file-1", Type: git2go.ErrDirectoryTraversal},
				},
			},
		},
		{
			desc: "mark created file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						chmodFileHeaderRequest("file-1", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "mark existing file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "move non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("non-existing", "should-not-be-created", true),
					},
					indexError:  "A file with this name doesn't exist",
					structError: &git2go.IndexError{Path: "non-existing", Type: git2go.ErrFileNotFound},
				},
			},
		},
		{
			desc: "move doesn't overwrite a file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						createFileHeaderRequest("file-2"),
						actionContentRequest("content-2"),
						moveFileHeaderRequest("file-1", "file-2", true),
					},
					indexError:  "A file with this name already exists",
					structError: &git2go.IndexError{Path: "file-2", Type: git2go.ErrFileExists},
				},
			},
		},
		{
			desc: "delete non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("non-existing"),
					},
					indexError:  "A file with this name doesn't exist",
					structError: &git2go.IndexError{Path: "non-existing", Type: git2go.ErrFileNotFound},
				},
			},
		},
		{
			desc: "delete file with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("../file-1"),
					},
					indexError:  "Path cannot include directory traversal",
					structError: &git2go.IndexError{Path: "../file-1", Type: git2go.ErrDirectoryTraversal},
				},
			},
		},
		{
			desc: "delete created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						deleteFileHeaderRequest("file-1"),
					},
					branchCreated: true,
					repoCreated:   true,
				},
			},
		},
		{
			desc: "delete existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("file-1"),
					},
				},
			},
		},
		{
			desc: "invalid action",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action: -1,
								},
							},
						}),
					},
					error: status.Error(codes.InvalidArgument, "NoMethodError: undefined method `downcase' for -1:Integer"),
				},
			},
		},
		{
			desc: "start repository refers to target repository",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startRepository: targetRepoSentinel,
					branchCreated:   true,
					repoCreated:     true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "empty target repository with start branch set",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startBranch:   "master",
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "start repository refers to an empty remote repository",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startBranch:     "master",
					startRepository: startRepo,
					branchCreated:   true,
					repoCreated:     true,
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			const branch = "main"

			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			for i, step := range tc.steps {
				if step.startRepository == targetRepoSentinel {
					step.startRepository = repo
				}

				headerRequest := headerRequest(
					repo,
					gittest.TestUser,
					branch,
					[]byte("commit message"),
					"",
					"",
				)
				setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))

				if step.startRepository != nil {
					setStartRepository(headerRequest, step.startRepository)
				}

				if step.startBranch != "" {
					setStartBranchName(headerRequest, []byte(step.startBranch))
				}

				stream, err := client.UserCommitFiles(ctx)
				require.NoError(t, err)
				require.NoError(t, stream.Send(headerRequest))

				for j, action := range step.actions {
					require.NoError(t, stream.Send(action), "step %d, action %d", i+1, j+1)
				}

				resp, err := stream.CloseAndRecv()

				if featureflag.UserCommitFilesStructuredErrors.IsEnabled(ctx) {
					if step.error != nil {
						testhelper.RequireGrpcError(t, step.error, err)
						continue
					}

					if step.unknownIndexError != nil {
						require.Equal(t, step.unknownIndexError, err)
						continue
					}

					if step.structError != nil {
						testhelper.RequireGrpcError(t, errWithDetails(t,
							step.structError.StructuredError(),
							&gitalypb.UserCommitFilesError{
								Error: &gitalypb.UserCommitFilesError_IndexUpdate{
									IndexUpdate: step.structError.Proto(),
								},
							},
						), err)
						continue
					}
				} else {
					testhelper.RequireGrpcError(t, step.error, err)
					if step.error != nil {
						continue
					}

					require.Equal(t, step.indexError, resp.IndexError, "step %d", i+1)
					if step.indexError != "" {
						continue
					}
				}

				require.Equal(t, step.branchCreated, resp.BranchUpdate.BranchCreated, "step %d", i+1)
				require.Equal(t, step.repoCreated, resp.BranchUpdate.RepoCreated, "step %d", i+1)
				gittest.RequireTree(t, cfg, repoPath, branch, step.treeEntries)

				authorDate := gittest.Exec(t, cfg, "-C", repoPath, "log", "--pretty='format:%ai'", "-1")
				require.Contains(t, string(authorDate), gittest.TimezoneOffset)
			}
		})
	}
}

func TestUserCommitFilesStableCommitID(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testUserCommitFilesStableCommitID)
}

func testUserCommitFilesStableCommitID(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for key, values := range testcfg.GitalyServersMetadataFromCfg(t, cfg) {
		for _, value := range values {
			ctx = metadata.AppendToOutgoingContext(ctx, key, value)
		}
	}

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)

	headerRequest := headerRequest(repoProto, gittest.TestUser, "master", []byte("commit message"), "", "")
	setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))
	setTimestamp(headerRequest, time.Unix(12345, 0))
	require.NoError(t, stream.Send(headerRequest))

	require.NoError(t, stream.Send(createFileHeaderRequest("file.txt")))
	require.NoError(t, stream.Send(actionContentRequest("content")))
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, resp.BranchUpdate.CommitId, "23ec4ccd7fcc6ecf39431805bbff1cbcb6c23b9d")
	require.True(t, resp.BranchUpdate.BranchCreated)
	require.True(t, resp.BranchUpdate.RepoCreated)
	gittest.RequireTree(t, cfg, repoPath, "refs/heads/master", []gittest.TreeEntry{
		{Mode: "100644", Path: "file.txt", Content: "content"},
	})

	commit, err := repo.ReadCommit(ctx, "refs/heads/master")
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:       "23ec4ccd7fcc6ecf39431805bbff1cbcb6c23b9d",
		TreeId:   "541550ddcf8a29bcd80b0800a142a7d47890cfd6",
		Subject:  []byte("commit message"),
		Body:     []byte("commit message"),
		BodySize: 14,
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Author Name"),
			Email:    []byte("author.email@example.com"),
			Date:     &timestamppb.Timestamp{Seconds: 12345},
			Timezone: []byte(gittest.TimezoneOffset),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     gittest.TestUser.Name,
			Email:    gittest.TestUser.Email,
			Date:     &timestamppb.Timestamp{Seconds: 12345},
			Timezone: []byte(gittest.TimezoneOffset),
		},
	}, commit)
}

func TestUserCommitFilesQuarantine(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testUserCommitFilesQuarantine)
}

func testUserCommitFilesQuarantine(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	// Set up a hook that parses the new object and then aborts the update. Like this, we can
	// assert that the object does not end up in the main repository.
	outputPath := filepath.Join(testhelper.TempDir(t), "output")
	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(
		`#!/bin/sh
		read oldval newval ref &&
		git rev-parse $newval^{commit} >%s &&
		exit 1
	`, outputPath)))

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)

	headerRequest := headerRequest(repoProto, gittest.TestUser, "master", []byte("commit message"), "", "")
	setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))
	setTimestamp(headerRequest, time.Unix(12345, 0))
	require.NoError(t, stream.Send(headerRequest))

	require.NoError(t, stream.Send(createFileHeaderRequest("file.txt")))
	require.NoError(t, stream.Send(actionContentRequest("content")))
	_, err = stream.CloseAndRecv()

	if featureflag.UserCommitFilesStructuredErrors.IsEnabled(ctx) {
		testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("denied by custom hooks").WithDetail(
			&gitalypb.UserCommitFilesError{
				Error: &gitalypb.UserCommitFilesError_CustomHook{
					CustomHook: &gitalypb.CustomHookError{
						HookType: gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
						Stdout:   []byte(""),
					},
				},
			},
		), err)
	} else {
		require.NoError(t, err)
	}

	hookOutput := testhelper.MustReadFile(t, outputPath)
	oid, err := git.ObjectHashSHA1.FromHex(text.ChompBytes(hookOutput))
	require.NoError(t, err)
	exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
	require.NoError(t, err)
	require.False(t, exists, "quarantined commit should have been discarded")
}

func TestSuccessfulUserCommitFilesFilesRequest(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testSuccessfulUserCommitFilesRequest)
}

func testSuccessfulUserCommitFilesRequest(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	newRepo, newRepoPath := gittest.CreateRepository(t, ctx, cfg)

	filePath := "héllo/wörld"
	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")
	testCases := []struct {
		desc            string
		repo            *gitalypb.Repository
		repoPath        string
		branchName      string
		startBranchName string
		expectedOldOID  string
		repoCreated     bool
		branchCreated   bool
		executeFilemode bool
		expectedError   error
	}{
		{
			desc:          "existing repo and branch",
			repo:          repo,
			repoPath:      repoPath,
			branchName:    "feature",
			repoCreated:   false,
			branchCreated: false,
		},
		{
			desc:           "existing repo and branch + expectedOldOID",
			repo:           repo,
			repoPath:       repoPath,
			branchName:     "wip",
			expectedOldOID: text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/wip")),
		},
		{
			desc:           "existing repo and branch + invalid expectedOldOID",
			repo:           repo,
			repoPath:       repoPath,
			branchName:     "few-commits",
			expectedOldOID: "foobar",
			expectedError: structerr.NewInvalidArgument(fmt.Sprintf(`invalid expected old object ID: invalid object ID: "foobar", expected length %v, got 6`, gittest.DefaultObjectHash.EncodedLen())).
				WithInterceptedMetadata("old_object_id", "foobar"),
		},
		{
			desc:           "existing repo and branch + valid expectedOldOID but invalid object",
			repo:           repo,
			repoPath:       repoPath,
			branchName:     "few-commits",
			expectedOldOID: gittest.DefaultObjectHash.ZeroOID.String(),
			expectedError: structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found").
				WithInterceptedMetadata("old_object_id", gittest.DefaultObjectHash.ZeroOID),
		},
		{
			desc:           "existing repo and branch + incorrect expectedOldOID",
			repo:           repo,
			repoPath:       repoPath,
			branchName:     "few-commits",
			expectedOldOID: text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/few-commits~1")),
			expectedError:  structerr.NewFailedPrecondition("Could not update refs/heads/few-commits. Please refresh and try again."),
		},
		{
			desc:          "existing repo, new branch",
			repo:          repo,
			repoPath:      repoPath,
			branchName:    "new-branch",
			repoCreated:   false,
			branchCreated: true,
		},
		{
			desc:            "existing repo, new branch, with start branch",
			repo:            repo,
			repoPath:        repoPath,
			branchName:      "new-branch-with-start-branch",
			startBranchName: "master",
			repoCreated:     false,
			branchCreated:   true,
		},
		{
			desc:          "new repo",
			repo:          newRepo,
			repoPath:      newRepoPath,
			branchName:    "feature",
			repoCreated:   true,
			branchCreated: true,
		},
		{
			desc:            "create executable file",
			repo:            repo,
			repoPath:        repoPath,
			branchName:      "feature-executable",
			repoCreated:     false,
			branchCreated:   true,
			executeFilemode: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			headerRequest := headerRequest(tc.repo, gittest.TestUser, tc.branchName, commitFilesMessage, tc.startBranchName, tc.expectedOldOID)
			setAuthorAndEmail(headerRequest, authorName, authorEmail)

			actionsRequest1 := createFileHeaderRequest(filePath)
			actionsRequest2 := actionContentRequest("My")
			actionsRequest3 := actionContentRequest(" content")
			actionsRequest4 := chmodFileHeaderRequest(filePath, tc.executeFilemode)

			stream, err := client.UserCommitFiles(ctx)

			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))
			require.NoError(t, stream.Send(actionsRequest2))
			require.NoError(t, stream.Send(actionsRequest3))
			require.NoError(t, stream.Send(actionsRequest4))

			resp, err := stream.CloseAndRecv()
			if tc.expectedError != nil {
				testhelper.RequireGrpcError(t, tc.expectedError, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.repoCreated, resp.GetBranchUpdate().GetRepoCreated())
			require.Equal(t, tc.branchCreated, resp.GetBranchUpdate().GetBranchCreated())

			headCommit, err := localrepo.NewTestRepo(t, cfg, tc.repo).ReadCommit(ctx, git.Revision(tc.branchName))
			require.NoError(t, err)
			require.Equal(t, authorName, headCommit.Author.Name)
			require.Equal(t, gittest.TestUser.Name, headCommit.Committer.Name)
			require.Equal(t, authorEmail, headCommit.Author.Email)
			require.Equal(t, gittest.TestUser.Email, headCommit.Committer.Email)
			require.Equal(t, commitFilesMessage, headCommit.Subject)

			fileContent := gittest.Exec(t, cfg, "-C", tc.repoPath, "show", headCommit.GetId()+":"+filePath)
			require.Equal(t, "My content", string(fileContent))

			commitInfo := gittest.Exec(t, cfg, "-C", tc.repoPath, "show", headCommit.GetId())
			expectedFilemode := "100644"
			if tc.executeFilemode {
				expectedFilemode = "100755"
			}
			require.Contains(t, string(commitInfo), fmt.Sprint("new file mode ", expectedFilemode))
		})
	}
}

func TestSuccessUserCommitFilesRequestMove(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testSuccessfulUserCommitFilesRequestMove)
}

func testSuccessfulUserCommitFilesRequestMove(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

	branchName := "master"
	previousFilePath := "README"
	filePath := "NEWREADME"
	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")

	for i, tc := range []struct {
		content string
		infer   bool
	}{
		{content: "", infer: false},
		{content: "foo", infer: false},
		{content: "", infer: true},
		{content: "foo", infer: true},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			testRepo, testRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})

			origFileContent := gittest.Exec(t, cfg, "-C", testRepoPath, "show", branchName+":"+previousFilePath)
			headerRequest := headerRequest(testRepo, gittest.TestUser, branchName, commitFilesMessage, "", "")
			setAuthorAndEmail(headerRequest, authorName, authorEmail)
			actionsRequest1 := moveFileHeaderRequest(previousFilePath, filePath, tc.infer)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))

			if len(tc.content) > 0 {
				actionsRequest2 := actionContentRequest(tc.content)
				require.NoError(t, stream.Send(actionsRequest2))
			}

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)

			update := resp.GetBranchUpdate()
			require.NotNil(t, update)

			fileContent := gittest.Exec(t, cfg, "-C", testRepoPath, "show", update.CommitId+":"+filePath)

			if tc.infer {
				require.Equal(t, string(origFileContent), string(fileContent))
			} else {
				require.Equal(t, tc.content, string(fileContent))
			}
		})
	}
}

func TestSuccessUserCommitFilesRequestForceCommit(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testSuccessfulUserCommitFilesRequestForceCommit)
}

func testSuccessfulUserCommitFilesRequestForceCommit(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")
	targetBranchName := "feature"
	startBranchName := []byte("master")

	startBranchCommit, err := repo.ReadCommit(ctx, git.Revision(startBranchName))
	require.NoError(t, err)

	targetBranchCommit, err := repo.ReadCommit(ctx, git.Revision(targetBranchName))
	require.NoError(t, err)

	mergeBaseOut := gittest.Exec(t, cfg, "-C", repoPath, "merge-base", targetBranchCommit.Id, startBranchCommit.Id)
	mergeBaseID := text.ChompBytes(mergeBaseOut)
	require.NotEqual(t, mergeBaseID, targetBranchCommit.Id, "expected %s not to be an ancestor of %s", targetBranchCommit.Id, startBranchCommit.Id)

	headerRequest := headerRequest(repoProto, gittest.TestUser, targetBranchName, commitFilesMessage, "", "")
	setAuthorAndEmail(headerRequest, authorName, authorEmail)
	setStartBranchName(headerRequest, startBranchName)
	setForce(headerRequest, true)

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
	require.NoError(t, stream.Send(actionContentRequest("Test")))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	update := resp.GetBranchUpdate()
	newTargetBranchCommit, err := repo.ReadCommit(ctx, git.Revision(targetBranchName))
	require.NoError(t, err)

	require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
	require.Equal(t, newTargetBranchCommit.ParentIds, []string{startBranchCommit.Id})
}

func TestSuccessUserCommitFilesRequestStartSha(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testSuccessfulUserCommitFilesRequestStartSha)
}

func testSuccessfulUserCommitFilesRequestStartSha(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, _, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	targetBranchName := "new"

	startCommit, err := repo.ReadCommit(ctx, "master")
	require.NoError(t, err)

	headerRequest := headerRequest(repoProto, gittest.TestUser, targetBranchName, commitFilesMessage, "", "")
	setStartSha(headerRequest, startCommit.Id)

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
	require.NoError(t, stream.Send(actionContentRequest("Test")))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	update := resp.GetBranchUpdate()
	newTargetBranchCommit, err := repo.ReadCommit(ctx, git.Revision(targetBranchName))
	require.NoError(t, err)

	require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
	require.Equal(t, newTargetBranchCommit.ParentIds, []string{startCommit.Id})
}

func TestSuccessUserCommitFilesRequestStartShaRemoteRepository(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testSuccessfulUserCommitFilesRequestStartShaRemoteRepository)
}

func testSuccessfulUserCommitFilesRequestStartShaRemoteRepository(t *testing.T, ctx context.Context) {
	t.Parallel()

	testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
		setStartSha(header, "1e292f8fedd741b75372e19097c76d327140c312")
	})
}

func TestSuccessUserCommitFilesRequestStartBranchRemoteRepository(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testSuccessfulUserCommitFilesRequestStartBranchRemoteRepository)
}

func testSuccessfulUserCommitFilesRequestStartBranchRemoteRepository(t *testing.T, ctx context.Context) {
	t.Parallel()

	testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
		setStartBranchName(header, []byte("master"))
	})
}

func testSuccessfulUserCommitFilesRemoteRepositoryRequest(setHeader func(header *gitalypb.UserCommitFilesRequest)) func(*testing.T, context.Context) {
	// Regular table driven test did not work here as there is some state shared in the helpers between the subtests.
	// Running them in different top level tests works, so we use a parameterized function instead to share the code.
	return func(t *testing.T, ctx context.Context) {
		ctx, cfg, repoProto, _, client := setupOperationsService(t, ctx)

		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		newRepoProto, _ := gittest.CreateRepository(t, ctx, cfg)
		newRepo := localrepo.NewTestRepo(t, cfg, newRepoProto)

		targetBranchName := "new"

		startCommit, err := repo.ReadCommit(ctx, "master")
		require.NoError(t, err)

		headerRequest := headerRequest(newRepoProto, gittest.TestUser, targetBranchName, commitFilesMessage, "", "")
		setHeader(headerRequest)
		setStartRepository(headerRequest, repoProto)

		stream, err := client.UserCommitFiles(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(headerRequest))
		require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
		require.NoError(t, stream.Send(actionContentRequest("Test")))

		resp, err := stream.CloseAndRecv()
		require.NoError(t, err)

		update := resp.GetBranchUpdate()
		newTargetBranchCommit, err := newRepo.ReadCommit(ctx, git.Revision(targetBranchName))
		require.NoError(t, err)

		require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
		require.Equal(t, newTargetBranchCommit.ParentIds, []string{startCommit.Id})
	}
}

func TestSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature)
}

func testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	targetBranchName := "master"

	testCases := []struct {
		desc   string
		user   *gitalypb.User
		author *gitalypb.CommitAuthor // expected value
	}{
		{
			desc:   "special characters at start and end",
			user:   &gitalypb.User{Name: []byte(".,:;<>\"'\nJane Doe.,:;<>'\"\n"), Email: []byte(".,:;<>'\"\njanedoe@gitlab.com.,:;<>'\"\n"), GlId: gittest.GlID},
			author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe"), Email: []byte("janedoe@gitlab.com")},
		},
		{
			desc:   "special characters in the middle",
			user:   &gitalypb.User{Name: []byte("Ja<ne\n D>oe"), Email: []byte("ja<ne\ndoe>@gitlab.com"), GlId: gittest.GlID},
			author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe"), Email: []byte("janedoe@gitlab.com")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			headerRequest := headerRequest(repoProto, tc.user, targetBranchName, commitFilesMessage, "", "")
			setAuthorAndEmail(headerRequest, tc.user.Name, tc.user.Email)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))

			_, err = stream.CloseAndRecv()
			require.NoError(t, err)

			newCommit, err := repo.ReadCommit(ctx, git.Revision(targetBranchName))
			require.NoError(t, err)

			require.Equal(t, tc.author.Name, newCommit.Author.Name, "author name")
			require.Equal(t, tc.author.Email, newCommit.Author.Email, "author email")
			require.Equal(t, tc.author.Name, newCommit.Committer.Name, "committer name")
			require.Equal(t, tc.author.Email, newCommit.Committer.Email, "committer email")
		})
	}
}

func TestFailedUserCommitFilesRequestDueToHooks(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testFailedUserCommitFilesRequestDueToHooks)
}

func testFailedUserCommitFilesRequestDueToHooks(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, _, repoProto, repoPath, client := setupOperationsService(t, ctx)

	branchName := "feature"
	filePath := "my/file.txt"
	headerRequest := headerRequest(repoProto, gittest.TestUser, branchName, commitFilesMessage, "", "")
	actionsRequest1 := createFileHeaderRequest(filePath)
	actionsRequest2 := actionContentRequest("My content")
	hookContent := []byte("#!/bin/sh\nprintenv | grep -e GL_ID -e GL_USERNAME | sort | paste -sd ' ' -\nexit 1")

	for _, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))
			require.NoError(t, stream.Send(actionsRequest2))

			resp, err := stream.CloseAndRecv()

			if featureflag.UserCommitFilesStructuredErrors.IsEnabled(ctx) {
				var hookT gitalypb.CustomHookError_HookType
				if hookName == "pre-receive" {
					hookT = gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE
				} else {
					hookT = gitalypb.CustomHookError_HOOK_TYPE_UPDATE
				}

				expectedOut := fmt.Sprintf("GL_ID=%s GL_USERNAME=%s\n",
					gittest.TestUser.GlId, gittest.TestUser.GlUsername)

				testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("denied by custom hooks").WithDetail(
					&gitalypb.UserCommitFilesError{
						Error: &gitalypb.UserCommitFilesError_CustomHook{
							CustomHook: &gitalypb.CustomHookError{
								HookType: hookT,
								Stdout:   []byte(expectedOut),
							},
						},
					},
				), err)
			} else {
				require.Contains(t, resp.PreReceiveError, "GL_ID="+gittest.TestUser.GlId)
				require.Contains(t, resp.PreReceiveError, "GL_USERNAME="+gittest.TestUser.GlUsername)
			}
		})
	}
}

func TestFailedUserCommitFilesRequestDueToIndexError(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testFailedUserCommitFilesRequestDueToIndexError)
}

func testFailedUserCommitFilesRequestDueToIndexError(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, _, repo, _, client := setupOperationsService(t, ctx)

	testCases := []struct {
		desc        string
		requests    []*gitalypb.UserCommitFilesRequest
		indexError  string
		structError *git2go.IndexError
	}{
		{
			desc: "file already exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(repo, gittest.TestUser, "feature", commitFilesMessage, "", ""),
				createFileHeaderRequest("README.md"),
				actionContentRequest("This file already exists"),
			},
			indexError:  "A file with this name already exists",
			structError: &git2go.IndexError{Path: "README.md", Type: git2go.ErrFileExists},
		},
		{
			desc: "file doesn't exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(repo, gittest.TestUser, "feature", commitFilesMessage, "", ""),
				chmodFileHeaderRequest("documents/story.txt", true),
			},
			indexError:  "A file with this name doesn't exist",
			structError: &git2go.IndexError{Path: "documents/story.txt", Type: git2go.ErrFileNotFound},
		},
		{
			desc: "dir already exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(repo, gittest.TestUser, "utf-dir", commitFilesMessage, "", ""),
				actionRequest(&gitalypb.UserCommitFilesAction{
					UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
						Header: &gitalypb.UserCommitFilesActionHeader{
							Action:        gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
							Base64Content: false,
							FilePath:      []byte("héllo"),
						},
					},
				}),
				actionContentRequest("This file already exists, as a directory"),
			},
			indexError:  "A directory with this name already exists",
			structError: &git2go.IndexError{Path: "héllo", Type: git2go.ErrDirectoryExists},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)

			for _, req := range tc.requests {
				require.NoError(t, stream.Send(req))
			}

			resp, err := stream.CloseAndRecv()
			if featureflag.UserCommitFilesStructuredErrors.IsEnabled(ctx) {
				testhelper.RequireGrpcError(t, errWithDetails(t,
					tc.structError.StructuredError(),
					&gitalypb.UserCommitFilesError{
						Error: &gitalypb.UserCommitFilesError_IndexUpdate{
							IndexUpdate: tc.structError.Proto(),
						},
					},
				), err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.indexError, resp.GetIndexError())
			}
		})
	}
}

func TestFailedUserCommitFilesRequest(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testFailedUserCommitFilesRequest)
}

func testFailedUserCommitFilesRequest(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, _, repo, _, client := setupOperationsService(t, ctx)

	branchName := "feature"

	testCases := []struct {
		desc        string
		req         *gitalypb.UserCommitFilesRequest
		expectedErr error
	}{
		{
			desc: "empty Repository",
			req:  headerRequest(nil, gittest.TestUser, branchName, commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:        "empty User",
			req:         headerRequest(repo, nil, branchName, commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "empty User"),
		},
		{
			desc:        "empty BranchName",
			req:         headerRequest(repo, gittest.TestUser, "", commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "empty BranchName"),
		},
		{
			desc:        "empty CommitMessage",
			req:         headerRequest(repo, gittest.TestUser, branchName, nil, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "empty CommitMessage"),
		},
		{
			desc:        "invalid object ID: \"foobar\"",
			req:         setStartSha(headerRequest(repo, gittest.TestUser, branchName, commitFilesMessage, "", ""), "foobar"),
			expectedErr: status.Error(codes.InvalidArgument, fmt.Sprintf(`invalid object ID: "foobar", expected length %v, got 6`, gittest.DefaultObjectHash.EncodedLen())),
		},
		{
			desc:        "failed to parse signature - Signature cannot have an empty name or email",
			req:         headerRequest(repo, &gitalypb.User{}, branchName, commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "commit: commit: failed to parse signature - Signature cannot have an empty name or email"),
		},
		{
			desc:        "failed to parse signature - Signature cannot have an empty name or email",
			req:         headerRequest(repo, &gitalypb.User{Name: []byte(""), Email: []byte("")}, branchName, commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "commit: commit: failed to parse signature - Signature cannot have an empty name or email"),
		},
		{
			desc:        "failed to parse signature - Signature cannot have an empty name or email",
			req:         headerRequest(repo, &gitalypb.User{Name: []byte(" "), Email: []byte(" ")}, branchName, commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "commit: commit: failed to parse signature - Signature cannot have an empty name or email"),
		},
		{
			desc:        "failed to parse signature - Signature cannot have an empty name or email",
			req:         headerRequest(repo, &gitalypb.User{Name: []byte("Jane Doe"), Email: []byte("")}, branchName, commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "commit: commit: failed to parse signature - Signature cannot have an empty name or email"),
		},
		{
			desc:        "failed to parse signature - Signature cannot have an empty name or email",
			req:         headerRequest(repo, &gitalypb.User{Name: []byte(""), Email: []byte("janedoe@gitlab.com")}, branchName, commitFilesMessage, "", ""),
			expectedErr: status.Error(codes.InvalidArgument, "commit: commit: failed to parse signature - Signature cannot have an empty name or email"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(tc.req))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestUserCommitFilesFailsIfRepositoryMissing(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UserCommitFilesStructuredErrors).Run(t,
		testUserCommitFilesFailsIfRepositoryMissing)
}

func testUserCommitFilesFailsIfRepositoryMissing(t *testing.T, ctx context.Context) {
	t.Parallel()
	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)
	repo := &gitalypb.Repository{
		StorageName:   cfg.Storages[0].Name,
		RelativePath:  t.Name(),
		GlRepository:  gittest.GlRepository,
		GlProjectPath: gittest.GlProjectPath,
	}

	branchName := "feature"
	req := headerRequest(repo, gittest.TestUser, branchName, commitFilesMessage, "", "")

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(req))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcCode(t, err, codes.NotFound)
	if testhelper.IsPraefectEnabled() {
		require.Contains(t, err.Error(), "mutator call: route repository mutator: get repository id")
		require.Contains(t, err.Error(), "not found")
	} else {
		require.Contains(t, err.Error(), "GetRepoPath: not a git repository")
	}
}

func headerRequest(repo *gitalypb.Repository, user *gitalypb.User, branchName string, commitMessage []byte, startBranchName string, expectedOldOID string) *gitalypb.UserCommitFilesRequest {
	return &gitalypb.UserCommitFilesRequest{
		UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Header{
			Header: &gitalypb.UserCommitFilesRequestHeader{
				Repository:      repo,
				User:            user,
				BranchName:      []byte(branchName),
				CommitMessage:   commitMessage,
				StartBranchName: []byte(startBranchName),
				StartRepository: nil,
				ExpectedOldOid:  expectedOldOID,
			},
		},
	}
}

func setAuthorAndEmail(headerRequest *gitalypb.UserCommitFilesRequest, authorName, authorEmail []byte) {
	header := getHeader(headerRequest)
	header.CommitAuthorName = authorName
	header.CommitAuthorEmail = authorEmail
}

func setTimestamp(headerRequest *gitalypb.UserCommitFilesRequest, time time.Time) {
	getHeader(headerRequest).Timestamp = timestamppb.New(time)
}

func setStartBranchName(headerRequest *gitalypb.UserCommitFilesRequest, startBranchName []byte) {
	header := getHeader(headerRequest)
	header.StartBranchName = startBranchName
}

func setStartRepository(headerRequest *gitalypb.UserCommitFilesRequest, startRepository *gitalypb.Repository) {
	header := getHeader(headerRequest)
	header.StartRepository = startRepository
}

func setStartSha(headerRequest *gitalypb.UserCommitFilesRequest, startSha string) *gitalypb.UserCommitFilesRequest {
	header := getHeader(headerRequest)
	header.StartSha = startSha

	return headerRequest
}

func setForce(headerRequest *gitalypb.UserCommitFilesRequest, force bool) {
	header := getHeader(headerRequest)
	header.Force = force
}

func getHeader(headerRequest *gitalypb.UserCommitFilesRequest) *gitalypb.UserCommitFilesRequestHeader {
	return headerRequest.UserCommitFilesRequestPayload.(*gitalypb.UserCommitFilesRequest_Header).Header
}

func createDirHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
				FilePath: []byte(filePath),
			},
		},
	})
}

func createFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_CREATE,
				Base64Content: false,
				FilePath:      []byte(filePath),
			},
		},
	})
}

func createBase64FileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_CREATE,
				Base64Content: true,
				FilePath:      []byte(filePath),
			},
		},
	})
}

func updateFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_UPDATE,
				FilePath: []byte(filePath),
			},
		},
	})
}

func updateBase64FileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_UPDATE,
				FilePath:      []byte(filePath),
				Base64Content: true,
			},
		},
	})
}

func chmodFileHeaderRequest(filePath string, executeFilemode bool) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:          gitalypb.UserCommitFilesActionHeader_CHMOD,
				FilePath:        []byte(filePath),
				ExecuteFilemode: executeFilemode,
			},
		},
	})
}

func moveFileHeaderRequest(previousPath, filePath string, infer bool) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:       gitalypb.UserCommitFilesActionHeader_MOVE,
				FilePath:     []byte(filePath),
				PreviousPath: []byte(previousPath),
				InferContent: infer,
			},
		},
	})
}

func deleteFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_DELETE,
				FilePath: []byte(filePath),
			},
		},
	})
}

func actionContentRequest(content string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Content{
			Content: []byte(content),
		},
	})
}

func actionRequest(action *gitalypb.UserCommitFilesAction) *gitalypb.UserCommitFilesRequest {
	return &gitalypb.UserCommitFilesRequest{
		UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Action{
			Action: action,
		},
	}
}
