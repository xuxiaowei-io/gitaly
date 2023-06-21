package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/signature"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

//go:generate rm -rf testdata/gpg-keys testdata/signing_gpg_key testdata/signing_gpg_key.pub
//go:generate mkdir -p testdata/gpg-keys
//go:generate chmod 0700 testdata/gpg-keys
//go:generate gpg --homedir testdata/gpg-keys --generate-key --batch testdata/genkey.in
//go:generate gpg --homedir testdata/gpg-keys --export --output testdata/signing_gpg_key.pub
//go:generate gpg --homedir testdata/gpg-keys --export-secret-keys --output testdata/signing_gpg_key

func TestWriteCommit(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(t, testWriteCommit)
}

func testWriteCommit(t *testing.T, ctx context.Context) {
	t.Helper()
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	testcfg.BuildGitalyGPG(t, cfg)

	repo := NewTestRepo(t, cfg, repoProto)

	blobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("something"))
	require.NoError(t, err)
	changedBlobID, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("changed"))
	require.NoError(t, err)

	treeEntryA := TreeEntry{Path: "file", Mode: "100644", OID: blobID}
	treeA := &TreeEntry{
		Type:    Tree,
		Mode:    "040000",
		Entries: []*TreeEntry{&treeEntryA},
	}
	require.NoError(t, treeA.Write(
		ctx,
		repo))

	treeB := &TreeEntry{
		Type: Tree,
		Mode: "040000",
		Entries: []*TreeEntry{
			{Path: "file", Mode: "100644", OID: changedBlobID},
		},
	}
	require.NoError(t, treeB.Write(ctx, repo))
	commitA, err := repo.WriteCommit(
		ctx,
		WriteCommitConfig{
			AuthorName:     "Tazmanian Devil",
			AuthorEmail:    "taz@devils.org",
			CommitterName:  "Tazmanian Devil",
			CommitterEmail: "taz@devils.org",
			Message:        "I ❤️  Tazmania",
			TreeID:         treeA.OID,
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
			TreeID:         treeB.OID,
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
				TreeID:         treeA.OID,
				AuthorName:     "Scrooge Mcduck",
				AuthorEmail:    "chief@ducks.org",
				CommitterName:  "Mickey Mouse",
				CommitterEmail: "mickey@mouse.org",
				AuthorDate:     commitDate,
				CommitterDate:  commitDate,
				Message:        "my custom message\n\ntrailer\n",
			},
			expectedCommit: strings.Join([]string{
				"tree " + string(treeA.OID),
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
				TreeID:         treeA.OID,
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
				"tree " + treeA.OID.String(),
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
				TreeID:         treeA.OID,
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
				"tree " + treeA.OID.String(),
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

	t.Run("signed", func(tb *testing.T) {
		if !featureflag.GPGSigning.IsEnabled(ctx) {
			tb.Skip()
		}

		cfg := WriteCommitConfig{
			TreeID:         treeA.OID,
			AuthorName:     gittest.DefaultCommitterName,
			AuthorEmail:    gittest.DefaultCommitterMail,
			CommitterName:  gittest.DefaultCommitterName,
			CommitterEmail: gittest.DefaultCommitterMail,
			AuthorDate:     gittest.DefaultCommitTime,
			CommitterDate:  gittest.DefaultCommitTime,
			Message:        "my custom message",
			SigningKey:     "testdata/signing_gpg_key",
		}

		oid, err := repo.WriteCommit(ctx, cfg)
		require.Nil(t, err)

		commit, err := repo.ReadCommit(ctx, git.Revision(oid))
		require.NoError(t, err)

		require.Equal(t, gittest.DefaultCommitAuthor, commit.Author)
		require.Equal(t, "my custom message", string(commit.Body))

		data, err := repo.ReadObject(ctx, oid)
		require.NoError(t, err)

		gpgsig, dataWithoutGpgSig := signature.ExtractSignature(t, ctx, data)

		pubKey := testhelper.MustReadFile(tb, "testdata/signing_gpg_key.pub")
		keyring, err := openpgp.ReadKeyRing(bytes.NewReader(pubKey))
		require.NoError(tb, err)

		_, err = openpgp.CheckArmoredDetachedSignature(
			keyring,
			strings.NewReader(dataWithoutGpgSig),
			strings.NewReader(gpgsig),
			&packet.Config{},
		)
		require.NoError(tb, err)
	})
}

func TestWriteCommit_validation(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.GPGSigning).Run(t, testWriteCommitValidation)
}

func testWriteCommitValidation(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := NewTestRepo(t, cfg, repoProto)

	blobID, err := repo.WriteBlob(ctx, "", strings.NewReader("foo"))
	require.NoError(t, err)
	tree := &TreeEntry{
		Type: Tree,
		Mode: "040000",
		Entries: []*TreeEntry{
			{
				OID:  blobID,
				Mode: "100644",
				Path: "file1",
			},
		},
	}
	require.NoError(t, tree.Write(ctx, repo))
	treeID := tree.OID

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
		{
			desc: "author name consists of invalid characters",
			cfg: WriteCommitConfig{
				TreeID:         treeID,
				AuthorName:     ".",
				AuthorEmail:    "author@example.com",
				CommitterName:  "Coe Mitter",
				CommitterEmail: "coemitter@example.com",
			},
			expectedErr: ErrDisallowedCharacters,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := repo.WriteCommit(ctx, tc.cfg)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
