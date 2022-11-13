//go:build !gitaly_test_sha256

package localrepo

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/tree"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestWriteCommit(t *testing.T) {
	t.Helper()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := git.CreateRepository(t, ctx, cfg, git.CreateRepositoryWriteCommitConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	blobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("something"))
	require.NoError(t, err)
	changedBlobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("changed"))
	require.NoError(t, err)

	treeEntryA := &tree.Entry{Path: "file", Mode: []byte("100644"), ObjectID: blobID, Type: tree.Blob}
	treeA, err := tree.Write(ctx, repo, []*tree.Entry{treeEntryA})
	require.NoError(t, err)

	treeB, err := repo.WriteTree(ctx, []*git.TreeEntry{
		{Path: "file", Mode: []byte("100644"), ObjectID: changedBlobID, Type: tree.Blob},
	})
	require.NoError(t, err)
	commitA, err := Write(
		ctx,
		repo,
		WriteCommitConfig{
			AuthorName:     "Tazmanian Devil",
			AuthorEmail:    "taz@devils.org",
			CommitterName:  "Tazmanian Devil",
			CommitterEmail: "taz@devils.org",
			Message:        "I ❤️  Tazmania",
			TreeID:         treeA,
		},
	)
	require.NoError(t, err)
	commitB, err := repo.WriteCommit(
		ctx,
		WriteCommitConfig{
			AuthorName:     "Daffy Duck",
			AuthorEmail:    "daffy@ducks.org",
			CommitterName:  "Daffy Duck",
			CommitterEmail: "daffy@ducks.org",
			Message:        "Big beak",
			TreeID:         treeB,
		},
	)
	require.NoError(t, err)

	commitDate := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		desc              string
		cfg               WriteCommitConfig
		expectedError     error
		expectedCommit    string
		expectedRevUpdate git.Revision
	}{
		{
			desc:          "missing tree",
			expectedError: ErrMissingTree,
		},
		{
			desc:          "missing author",
			expectedError: ErrMissingAuthor,
			cfg: WriteCommitConfig{
				TreeID: treeA,
			},
		},
		{
			desc:          "missing committer",
			expectedError: ErrMissingCommitter,
			cfg: WriteCommitConfig{
				TreeID:     treeA,
				AuthorName: "Scrooge Mcduck",
			},
		},
		{
			desc:          "missing author email",
			expectedError: ErrMissingAuthorEmail,
			cfg: WriteCommitConfig{
				TreeID:        treeA,
				AuthorName:    "Scrooge Mcduck",
				CommitterName: "Mickey Mouse",
			},
		},
		{
			desc:          "missing committer email",
			expectedError: ErrMissingCommitterEmail,
			cfg: WriteCommitConfig{
				TreeID:        treeA,
				AuthorName:    "Scrooge Mcduck",
				AuthorEmail:   "chief@ducks.org",
				CommitterName: "Mickey Mouse",
			},
		},
		{
			desc: "with commit message",
			cfg: WriteCommitConfig{
				TreeID:         treeA,
				AuthorName:     "Scrooge Mcduck",
				AuthorEmail:    "chief@ducks.org",
				CommitterName:  "Mickey Mouse",
				CommitterEmail: "mickey@mouse.org",
				AuthorDate:     commitDate,
				CommitterDate:  commitDate,
				Message:        "my custom message\n\ntrailer\n",
			},
			expectedCommit: strings.Join([]string{
				"tree " + string(treeA),
				fmt.Sprintf(
					"author Scrooge Mcduck <chief@ducks.org> %d %s",
					commitDate.Unix(),
					commitDate.Format("-0700"),
				),
				fmt.Sprintf(
					"committer Mickey Mouse <mickey@mouse.org> %d %s",
					commitDate.Unix(),
					commitDate.Format("-0700"),
				),
				"",
				"my custom message",
				"",
				"trailer",
			}, "\n"),
		},
		{
			desc: "with multiple parents",
			cfg: WriteCommitConfig{
				TreeID:         treeA,
				Parents:        []git.ObjectID{commitA, commitB},
				AuthorName:     "Scrooge Mcduck",
				AuthorEmail:    "chief@ducks.org",
				CommitterName:  "Mickey Mouse",
				CommitterEmail: "mickey@mouse.org",
				AuthorDate:     commitDate,
				CommitterDate:  commitDate,
				Message:        "my custom message",
			},
			expectedCommit: strings.Join([]string{
				"tree " + treeA.String(),
				"parent " + commitA.String(),
				"parent " + commitB.String(),
				fmt.Sprintf(
					"author Scrooge Mcduck <chief@ducks.org> %d %s",
					commitDate.Unix(),
					commitDate.Format("-0700"),
				),
				fmt.Sprintf(
					"committer Mickey Mouse <mickey@mouse.org> %d %s",
					commitDate.Unix(),
					commitDate.Format("-0700"),
				),
				"",
				"my custom message",
			}, "\n"),
		},
		{
			desc: "with reference",
			cfg: WriteCommitConfig{
				TreeID:         treeA,
				Parents:        []git.ObjectID{commitA, commitB},
				AuthorName:     "Scrooge Mcduck",
				AuthorEmail:    "chief@ducks.org",
				CommitterName:  "Mickey Mouse",
				CommitterEmail: "mickey@mouse.org",
				AuthorDate:     commitDate,
				CommitterDate:  commitDate,
				Message:        "my custom message",
				Reference:      "refs/heads/foo",
			},
			expectedCommit: strings.Join([]string{
				"tree " + treeA.String(),
				"parent " + commitA.String(),
				"parent " + commitB.String(),
				fmt.Sprintf(
					"author Scrooge Mcduck <chief@ducks.org> %d %s",
					commitDate.Unix(),
					commitDate.Format("-0700"),
				),
				fmt.Sprintf(
					"committer Mickey Mouse <mickey@mouse.org> %d %s",
					commitDate.Unix(),
					commitDate.Format("-0700"),
				),
				"",
				"my custom message",
			}, "\n"),
			expectedRevUpdate: "refs/heads/foo",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := repo.WriteCommit(ctx, tc.cfg)
			require.Equal(t, tc.expectedError, err)
			if err != nil {
				return
			}

			commit, err := repo.ReadObject(ctx, oid)
			require.NoError(t, err)

			require.Equal(t, tc.expectedCommit, text.ChompBytes(commit))

			if tc.expectedRevUpdate != "" {
				updatedOID := git.Exec(t, cfg, "-C", repoPath, "rev-parse", tc.expectedRevUpdate.String())
				require.Equal(t, oid, git.ObjectID(text.ChompBytes(updatedOID)))
			}
		})
	}
}
