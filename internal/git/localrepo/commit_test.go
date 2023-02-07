package localrepo

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestWriteCommit(t *testing.T) {
	t.Helper()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := NewTestRepo(t, cfg, repoProto)

	blobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("something"))
	require.NoError(t, err)
	changedBlobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("changed"))
	require.NoError(t, err)

	treeEntryA := TreeEntry{Path: "file", Mode: "100644", OID: blobID}
	treeA, err := repo.WriteTree(ctx, []TreeEntry{treeEntryA})
	require.NoError(t, err)

	treeB, err := repo.WriteTree(ctx, []TreeEntry{
		{Path: "file", Mode: "100644", OID: changedBlobID},
	})
	require.NoError(t, err)
	commitA, err := repo.WriteCommit(
		ctx,
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
				updatedOID := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", tc.expectedRevUpdate.String())
				require.Equal(t, oid, git.ObjectID(text.ChompBytes(updatedOID)))
			}
		})
	}
}

func TestWriteCommit_validation(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := NewTestRepo(t, cfg, repoProto)

	blobID, err := repo.WriteBlob(ctx, "", strings.NewReader("foo"))
	require.NoError(t, err)
	treeID, err := repo.WriteTree(ctx, []TreeEntry{
		{
			OID:  blobID,
			Mode: "100644",
			Path: "file1",
		},
	})
	require.NoError(t, err)

	testCases := []struct {
		desc        string
		cfg         WriteCommitConfig
		expectedErr error
	}{
		{
			desc: "no author name",
			cfg: WriteCommitConfig{
				TreeID:         treeID,
				AuthorEmail:    "author@example.com",
				CommitterName:  "Coe Mitter",
				CommitterEmail: "coemitter@example.com",
			},
			expectedErr: ErrMissingAuthorName,
		},
		{
			desc: "no committer name",
			cfg: WriteCommitConfig{
				TreeID:         treeID,
				AuthorName:     "Awe Thor",
				AuthorEmail:    "author@example.com",
				CommitterEmail: "coemitter@example.com",
			},
			expectedErr: ErrMissingCommitterName,
		},
		{
			desc: "no tree",
			cfg: WriteCommitConfig{
				AuthorName:     "Awe Thor",
				AuthorEmail:    "author@example.com",
				CommitterName:  "Coe Mitter",
				CommitterEmail: "coemitter@example.com",
			},
			expectedErr: ErrMissingTree,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := repo.WriteCommit(ctx, tc.cfg)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
