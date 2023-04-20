//go:build !gitaly_test_sha256

package repository

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSearchFilesByContent(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "a", Mode: "100644", Content: "a-1\na-2\na-3\na-4\na-5\na-6\n"},
		{Path: "b", Mode: "100644", Content: "b-1\nb-2\nb-3\nb-4\nb-5\nb-6\n"},
		{Path: "dir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "c", Mode: "100644", Content: "c-1\nc-2\nc-3\nc-4\nc-5\nc-6\n"},
		})},
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"), gittest.WithTree(treeID))

	largeFilesCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "huge-file", Mode: "100644", Content: strings.Repeat("abcdefghi\n", 210000)},
		gittest.TreeEntry{Path: "huge-utf8", Mode: "100644", Content: strings.Repeat("你见天吃了什么东西?\n", 70000)},
	))

	generateMatch := func(ref, file string, from, to int, generateLine func(line int) string) string {
		var builder strings.Builder
		for i := from; i <= to; i++ {
			_, err := builder.WriteString(fmt.Sprintf("%s:%s\x00%d\x00%s\n", ref, file, i, generateLine(i)))
			require.NoError(t, err)
		}
		return builder.String()
	}
	prefixedLine := func(prefix string) func(int) string {
		return func(line int) string { return fmt.Sprintf("%s-%d", prefix, line) }
	}
	staticLine := func(content string) func(int) string {
		return func(int) string { return content }
	}

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.SearchFilesByContentRequest
		expectedErr     error
		expectedMatches []string
	}{
		{
			desc: "no repo",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "empty request",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
			},
			expectedErr: structerr.NewInvalidArgument("no query given"),
		},
		{
			desc: "only query given",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      "foo",
			},
			expectedErr: structerr.NewInvalidArgument("no ref given"),
		},
		{
			desc: "invalid ref argument",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository: repoProto,
				Query:      ".",
				Ref:        []byte("--no-index"),
			},
			expectedErr: structerr.NewInvalidArgument("invalid ref argument"),
		},
		{
			desc: "single matching file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:      repoProto,
				Query:           "a-6",
				Ref:             []byte("branch"),
				ChunkedResponse: true,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
			},
		},
		{
			desc: "multiple matching files",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:      repoProto,
				Query:           "6",
				Ref:             []byte("branch"),
				ChunkedResponse: true,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 4, 6, prefixedLine("a")),
				generateMatch("branch", "b", 4, 6, prefixedLine("b")),
				generateMatch("branch", "dir/c", 4, 6, prefixedLine("c")),
			},
		},
		{
			desc: "multiple matching files with multiple matching lines",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:      repoProto,
				Query:           "[ac]-[16]",
				Ref:             []byte("branch"),
				ChunkedResponse: true,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 1, 6, prefixedLine("a")),
				generateMatch("branch", "dir/c", 1, 6, prefixedLine("c")),
			},
		},
		{
			desc: "no results",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:      repoProto,
				Query:           "这个应该没有结果",
				Ref:             []byte("branch"),
				ChunkedResponse: true,
			},
			expectedMatches: nil,
		},
		{
			desc: "with regexp limiter only recognized by pcre",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:      repoProto,
				Query:           "(*LIMIT_MATCH=1)a",
				Ref:             []byte("branch"),
				ChunkedResponse: true,
			},
			expectedMatches: []string{
				generateMatch("branch", "a", 1, 6, prefixedLine("a")),
			},
		},
		{
			desc: "large file",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:      repoProto,
				Query:           "abcdefg",
				Ref:             []byte(largeFilesCommit),
				ChunkedResponse: true,
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-file", 1, 210000, staticLine("abcdefghi")),
			},
		},
		{
			desc: "large file with unicode",
			request: &gitalypb.SearchFilesByContentRequest{
				Repository:      repoProto,
				Query:           "什么东西",
				Ref:             []byte(largeFilesCommit),
				ChunkedResponse: true,
			},
			expectedMatches: []string{
				generateMatch(largeFilesCommit.String(), "huge-utf8", 1, 70000, staticLine("你见天吃了什么东西?")),
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.SearchFilesByContent(ctx, tc.request)
			require.NoError(t, err)

			matches, err := consumeFilenameByContentChunked(stream)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedMatches, matches)
		})
	}
}

func TestSearchFilesByNameSuccessful(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, _, client := setupRepositoryService(t, ctx)

	testCases := []struct {
		desc     string
		ref      []byte
		query    string
		filter   string
		numFiles int
		testFile []byte
	}{
		{
			desc:     "one file",
			ref:      []byte("many_files"),
			query:    "files/images/logo-black.png",
			numFiles: 1,
			testFile: []byte("files/images/logo-black.png"),
		},
		{
			desc:     "many files",
			ref:      []byte("many_files"),
			query:    "many_files",
			numFiles: 1001,
			testFile: []byte("many_files/99"),
		},
		{
			desc:     "filtered",
			ref:      []byte("many_files"),
			query:    "files/images",
			filter:   `\.svg$`,
			numFiles: 1,
			testFile: []byte("files/images/wm.svg"),
		},
		{
			desc:     "non-existent ref",
			query:    ".",
			ref:      []byte("non_existent_ref"),
			numFiles: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.SearchFilesByName(ctx, &gitalypb.SearchFilesByNameRequest{
				Repository: repo,
				Ref:        tc.ref,
				Query:      tc.query,
				Filter:     tc.filter,
			})
			require.NoError(t, err)

			var files [][]byte
			files, err = consumeFilenameByName(stream)
			require.NoError(t, err)

			require.Equal(t, tc.numFiles, len(files))
			if tc.numFiles != 0 {
				require.Contains(t, files, tc.testFile)
			}
		})
	}
}

func TestSearchFilesByNameUnusualFileNamesSuccessful(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	ref := []byte("unusual_file_names")
	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(string(ref)),
		gittest.WithMessage("commit message"),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "\"file with quote.txt", Mode: "100644", Content: "something"},
			gittest.TreeEntry{Path: ".vimrc", Mode: "100644", Content: "something"},
			gittest.TreeEntry{Path: "cuộc đời là những chuyến đi.md", Mode: "100644", Content: "something"},
			gittest.TreeEntry{Path: "编码 'foo'.md", Mode: "100644", Content: "something"},
		),
	)

	testCases := []struct {
		desc          string
		query         string
		filter        string
		expectedFiles [][]byte
	}{
		{
			desc:          "file with quote",
			query:         "\"file with quote.txt",
			expectedFiles: [][]byte{[]byte("\"file with quote.txt")},
		},
		{
			desc:          "dotfiles",
			query:         ".vimrc",
			expectedFiles: [][]byte{[]byte(".vimrc")},
		},
		{
			desc:          "latin-base language",
			query:         "cuộc đời là những chuyến đi.md",
			expectedFiles: [][]byte{[]byte("cuộc đời là những chuyến đi.md")},
		},
		{
			desc:          "non-latin language",
			query:         "编码 'foo'.md",
			expectedFiles: [][]byte{[]byte("编码 'foo'.md")},
		},
		{
			desc:          "filter file with quote",
			query:         ".",
			filter:        "^\"file.*",
			expectedFiles: [][]byte{[]byte("\"file with quote.txt")},
		},
		{
			desc:          "filter dotfiles",
			query:         ".",
			filter:        "^\\..*",
			expectedFiles: [][]byte{[]byte(".vimrc")},
		},
		{
			desc:          "filter latin-base language",
			query:         ".",
			filter:        "cuộc đời .*\\.md",
			expectedFiles: [][]byte{[]byte("cuộc đời là những chuyến đi.md")},
		},
		{
			desc:          "filter non-latin language",
			query:         ".",
			filter:        "编码 'foo'\\.(md|txt|rdoc)",
			expectedFiles: [][]byte{[]byte("编码 'foo'.md")},
		},
		{
			desc:   "wildcard filter",
			query:  ".",
			filter: ".*",
			expectedFiles: [][]byte{
				[]byte("\"file with quote.txt"),
				[]byte(".vimrc"),
				[]byte("cuộc đời là những chuyến đi.md"),
				[]byte("编码 'foo'.md"),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.SearchFilesByName(ctx, &gitalypb.SearchFilesByNameRequest{
				Repository: repo,
				Ref:        ref,
				Query:      tc.query,
				Filter:     tc.filter,
			})
			require.NoError(t, err)

			var files [][]byte
			files, err = consumeFilenameByName(stream)
			require.NoError(t, err)
			require.Equal(t, tc.expectedFiles, files)
		})
	}
}

func TestSearchFilesByNamePaginationSuccessful(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	ref := []byte("pagination")
	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(string(ref)),
		gittest.WithMessage("commit message"),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "file1.md", Mode: "100644", Content: "file1"},
			gittest.TreeEntry{Path: "file2.md", Mode: "100644", Content: "file2"},
			gittest.TreeEntry{Path: "file3.md", Mode: "100644", Content: "file3"},
			gittest.TreeEntry{Path: "file4.md", Mode: "100644", Content: "file4"},
			gittest.TreeEntry{Path: "file5.md", Mode: "100644", Content: "file5"},
			gittest.TreeEntry{Path: "new_file1.md", Mode: "100644", Content: "new_file1"},
			gittest.TreeEntry{Path: "new_file2.md", Mode: "100644", Content: "new_file2"},
			gittest.TreeEntry{Path: "new_file3.md", Mode: "100644", Content: "new_file3"},
		),
	)

	testCases := []struct {
		desc          string
		query         string
		filter        string
		offset        uint32
		limit         uint32
		expectedFiles [][]byte
	}{
		{
			desc:          "only limit is set",
			query:         ".",
			limit:         3,
			expectedFiles: [][]byte{[]byte("file1.md"), []byte("file2.md"), []byte("file3.md")},
		},
		{
			desc:          "only offset is set",
			query:         ".",
			offset:        2,
			expectedFiles: [][]byte{[]byte("file3.md"), []byte("file4.md"), []byte("file5.md"), []byte("new_file1.md"), []byte("new_file2.md"), []byte("new_file3.md")},
		},
		{
			desc:          "both limit and offset are set",
			query:         ".",
			offset:        2,
			limit:         2,
			expectedFiles: [][]byte{[]byte("file3.md"), []byte("file4.md")},
		},
		{
			desc:          "offset exceeds the total files",
			query:         ".",
			offset:        8,
			expectedFiles: nil,
		},
		{
			desc:          "offset + limit exceeds the total files",
			query:         ".",
			offset:        6,
			limit:         5,
			expectedFiles: [][]byte{[]byte("new_file2.md"), []byte("new_file3.md")},
		},
		{
			desc:          "limit and offset combine with filter",
			query:         ".",
			filter:        "new.*",
			offset:        1,
			limit:         2,
			expectedFiles: [][]byte{[]byte("new_file2.md"), []byte("new_file3.md")},
		},
		{
			desc:          "limit and offset combine with unmatched filter",
			query:         ".",
			filter:        "not_matched.*",
			offset:        1,
			limit:         2,
			expectedFiles: nil,
		},
		{
			desc:          "limit and offset combine with matched query",
			query:         "new_file2.md",
			offset:        0,
			limit:         2,
			expectedFiles: [][]byte{[]byte("new_file2.md")},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.SearchFilesByName(ctx, &gitalypb.SearchFilesByNameRequest{
				Repository: repo,
				Ref:        ref,
				Query:      tc.query,
				Filter:     tc.filter,
				Offset:     tc.offset,
				Limit:      tc.limit,
			})
			require.NoError(t, err)

			var files [][]byte
			files, err = consumeFilenameByName(stream)
			require.NoError(t, err)
			require.Equal(t, tc.expectedFiles, files)
		})
	}
}

func TestSearchFilesByNameFailure(t *testing.T) {
	t.Parallel()
	cfg := testcfg.Build(t)
	gitCommandFactory := gittest.NewCommandFactory(t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	locator := config.NewLocator(cfg)

	connsPool := client.NewPool()
	defer testhelper.MustClose(t, connsPool)

	git2goExecutor := git2go.NewExecutor(cfg, gitCommandFactory, locator)
	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())
	housekeepingManager := housekeeping.NewManager(cfg.Prometheus, txManager)

	server := NewServer(
		cfg,
		locator,
		txManager,
		gitCommandFactory,
		catfileCache,
		connsPool,
		git2goExecutor,
		housekeepingManager,
	)

	testCases := []struct {
		desc   string
		repo   *gitalypb.Repository
		query  string
		filter string
		ref    string
		offset uint32
		limit  uint32
		code   codes.Code
		msg    string
	}{
		{
			desc: "repository not initialized",
			repo: &gitalypb.Repository{},
			code: codes.InvalidArgument,
			msg:  "empty StorageName",
		},
		{
			desc: "empty request",
			repo: &gitalypb.Repository{RelativePath: "stub", StorageName: "stub"},
			code: codes.InvalidArgument,
			msg:  "no query given",
		},
		{
			desc:  "only query given",
			repo:  &gitalypb.Repository{RelativePath: "stub", StorageName: "stub"},
			query: "foo",
			code:  codes.InvalidArgument,
			msg:   "no ref given",
		},
		{
			desc:  "no repo",
			query: "foo",
			ref:   "master",
			code:  codes.InvalidArgument,
			msg:   "empty Repo",
		},
		{
			desc:   "invalid filter",
			repo:   &gitalypb.Repository{RelativePath: "stub", StorageName: "stub"},
			query:  "foo",
			ref:    "master",
			filter: "+*.",
			code:   codes.InvalidArgument,
			msg:    "filter did not compile: error parsing regexp:",
		},
		{
			desc:   "filter longer than max",
			repo:   &gitalypb.Repository{RelativePath: "stub", StorageName: "stub"},
			query:  "foo",
			ref:    "master",
			filter: strings.Repeat(".", searchFilesFilterMaxLength+1),
			code:   codes.InvalidArgument,
			msg:    "filter exceeds maximum length",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := server.SearchFilesByName(&gitalypb.SearchFilesByNameRequest{
				Repository: tc.repo,
				Query:      tc.query,
				Filter:     tc.filter,
				Ref:        []byte(tc.ref),
				Offset:     tc.offset,
				Limit:      tc.limit,
			}, nil)

			testhelper.RequireGrpcCode(t, err, tc.code)
			require.Contains(t, err.Error(), tc.msg)
		})
	}
}

func consumeFilenameByContentChunked(stream gitalypb.RepositoryService_SearchFilesByContentClient) ([]string, error) {
	var matches []string
	var match []byte

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		match = append(match, resp.MatchData...)
		if resp.EndOfMatch {
			matches = append(matches, string(match))
			match = nil
		}
	}

	return matches, nil
}

func consumeFilenameByName(stream gitalypb.RepositoryService_SearchFilesByNameClient) ([][]byte, error) {
	var ret [][]byte

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ret = append(ret, resp.Files...)
	}
	return ret, nil
}
