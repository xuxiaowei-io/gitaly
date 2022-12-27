package catfile

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func TestGetCommit(t *testing.T) {
	ctx := testhelper.Context(t)
	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})

	cfg, objectReader, _, repoPath := setupObjectReader(t, ctx)

	blobID := git.WriteBlob(t, cfg, repoPath, []byte("data"))
	treeID := git.WriteTree(t, cfg, repoPath, []git.TreeEntry{
		{Path: "file", Mode: "100644", OID: blobID},
	})
	commitID := writeTestCommit(
		t,
		cfg,
		repoPath,
		withMessage("commit message\n\ncommit body"), withTree(treeID),
	)

	for _, tc := range []struct {
		desc           string
		revision       string
		expectedErr    error
		expectedCommit *gitalypb.GitCommit
	}{
		{
			desc:     "commit",
			revision: commitID.String(),
			expectedCommit: &gitalypb.GitCommit{
				Id:        commitID.String(),
				TreeId:    treeID.String(),
				Author:    git.DefaultCommitAuthor,
				Committer: git.DefaultCommitAuthor,
				Body:      []byte("commit message\n\ncommit body"),
				BodySize:  27,
				Subject:   []byte("commit message"),
			},
		},
		{
			desc:        "not existing commit",
			revision:    "not existing revision",
			expectedErr: NotFoundError{errors.New("object not found")},
		},
		{
			desc:        "blob sha",
			revision:    blobID.String(),
			expectedErr: NotFoundError{errors.New("object not found")},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			commit, err := GetCommit(ctx, objectReader, git.Revision(tc.revision))
			require.Equal(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedCommit, commit)
		})
	}
}

func TestGetCommitWithTrailers(t *testing.T) {
	ctx := testhelper.Context(t)
	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})

	cfg, objectReader, repo, repoPath := setupObjectReader(t, ctx)

	commitID := writeTestCommit(t, cfg, repoPath, withMessage(
		"some header\n"+
			"\n"+
			"Commit message.\n"+
			"\n"+
			"Signed-off-by: John Doe <john.doe@example.com>\n"+
			"Signed-off-by: Jane Doe <jane.doe@example.com>\n",
	))

	commit, err := GetCommitWithTrailers(ctx, git.NewCommandFactory(t, cfg), repo, objectReader, commitID.Revision())

	require.NoError(t, err)

	require.Equal(t, commit.Trailers, []*gitalypb.CommitTrailer{
		{
			Key:   []byte("Signed-off-by"),
			Value: []byte("John Doe <john.doe@example.com>"),
		},
		{
			Key:   []byte("Signed-off-by"),
			Value: []byte("Jane Doe <jane.doe@example.com>"),
		},
	})
}
