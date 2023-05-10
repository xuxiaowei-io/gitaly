//go:build !gitaly_test_sha256

package git2go

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestExecutor_Commit(t *testing.T) {
	const (
		DefaultMode    = "100644"
		ExecutableMode = "100755"
	)

	type step struct {
		actions     []Action
		error       error
		treeEntries []gittest.TreeEntry
		testCommit  func(testing.TB, git.ObjectID)
	}
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyGit2Go(t, cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	originalFile, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("original"))
	require.NoError(t, err)

	updatedFile, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("updated"))
	require.NoError(t, err)

	executor := NewExecutor(cfg, gittest.NewCommandFactory(t, cfg), config.NewLocator(cfg))

	for _, tc := range []struct {
		desc           string
		steps          []step
		signingKeyPath string
	}{
		{
			desc: "create directory",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory created duplicate",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
						CreateDirectory{Path: "directory"},
					},
					error: IndexError{Type: ErrDirectoryExists, Path: "directory"},
				},
			},
		},
		{
			desc: "create directory existing duplicate",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory/.gitkeep"},
					},
				},
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
					},
					error: IndexError{Type: ErrDirectoryExists, Path: "directory"},
				},
			},
		},
		{
			desc: "create directory with a files name",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
				{
					actions: []Action{
						CreateDirectory{Path: "file"},
					},
					error: IndexError{Type: ErrFileExists, Path: "file"},
				},
			},
		},
		{
			desc: "create file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "create duplicate file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
						CreateFile{Path: "file", OID: updatedFile.String()},
					},
					error: IndexError{Type: ErrFileExists, Path: "file"},
				},
			},
		},
		{
			desc: "create file overwrites directory",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
						CreateFile{Path: "directory", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory", Content: "original"},
					},
				},
			},
		},
		{
			desc: "update created file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
						UpdateFile{Path: "file", OID: updatedFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "update existing file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
				{
					actions: []Action{
						UpdateFile{Path: "file", OID: updatedFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "update non-existing file",
			steps: []step{
				{
					actions: []Action{
						UpdateFile{Path: "non-existing", OID: updatedFile.String()},
					},
					error: IndexError{Type: ErrFileNotFound, Path: "non-existing"},
				},
			},
		},
		{
			desc: "move created file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "original-file", OID: originalFile.String()},
						MoveFile{Path: "original-file", NewPath: "moved-file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "moving directory fails",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
						MoveFile{Path: "directory", NewPath: "moved-directory"},
					},
					error: IndexError{Type: ErrFileNotFound, Path: "directory"},
				},
			},
		},
		{
			desc: "move file inferring content",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "original-file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original"},
					},
				},
				{
					actions: []Action{
						MoveFile{Path: "original-file", NewPath: "moved-file"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move file with non-existing source",
			steps: []step{
				{
					actions: []Action{
						MoveFile{Path: "non-existing", NewPath: "destination-file"},
					},
					error: IndexError{Type: ErrFileNotFound, Path: "non-existing"},
				},
			},
		},
		{
			desc: "move file with already existing destination file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "source-file", OID: originalFile.String()},
						CreateFile{Path: "already-existing", OID: updatedFile.String()},
						MoveFile{Path: "source-file", NewPath: "already-existing"},
					},
					error: IndexError{Type: ErrFileExists, Path: "already-existing"},
				},
			},
		},
		{
			// seems like a bug in the original implementation to allow overwriting a
			// directory
			desc: "move file with already existing destination directory",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
						CreateDirectory{Path: "already-existing"},
						MoveFile{Path: "file", NewPath: "already-existing"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "already-existing", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move file providing content",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "original-file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original"},
					},
				},
				{
					actions: []Action{
						MoveFile{Path: "original-file", NewPath: "moved-file", OID: updatedFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "mark non-existing file executable",
			steps: []step{
				{
					actions: []Action{
						ChangeFileMode{Path: "non-existing"},
					},
					error: IndexError{Type: ErrFileNotFound, Path: "non-existing"},
				},
			},
		},
		{
			desc: "mark executable file executable",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []Action{
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "mark created file executable",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "mark existing file executable",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []Action{
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move non-existing file",
			steps: []step{
				{
					actions: []Action{
						MoveFile{Path: "non-existing", NewPath: "destination"},
					},
					error: IndexError{Type: ErrFileNotFound, Path: "non-existing"},
				},
			},
		},
		{
			desc: "move doesn't overwrite a file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						CreateFile{Path: "file-2", OID: updatedFile.String()},
						MoveFile{Path: "file-1", NewPath: "file-2"},
					},
					error: IndexError{Type: ErrFileExists, Path: "file-2"},
				},
			},
		},
		{
			desc: "delete non-existing file",
			steps: []step{
				{
					actions: []Action{
						DeleteFile{Path: "non-existing"},
					},
					error: IndexError{Type: ErrFileNotFound, Path: "non-existing"},
				},
			},
		},
		{
			desc: "delete created file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						DeleteFile{Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "delete existing file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []Action{
						DeleteFile{Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "update created file, sign commit and verify signature using GPG signing key",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
						UpdateFile{Path: "file", OID: updatedFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "updated"},
					},
					testCommit: func(tb testing.TB, commitID git.ObjectID) {
						gpgsig, dataWithoutGpgSig := extractSignature(t, ctx, repo, commitID)

						file, err := os.Open("testdata/signing_gpg_key.pub")
						require.NoError(tb, err)
						defer testhelper.MustClose(tb, file)

						keyring, err := openpgp.ReadKeyRing(file)
						require.NoError(tb, err)

						_, err = openpgp.CheckArmoredDetachedSignature(
							keyring,
							strings.NewReader(dataWithoutGpgSig),
							strings.NewReader(gpgsig),
							&packet.Config{},
						)
						require.NoError(tb, err)
					},
				},
			},
			signingKeyPath: "testdata/signing_gpg_key",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			author := defaultCommitAuthorSignature()
			committer := defaultCommitAuthorSignature()

			executor.signingKey = tc.signingKeyPath

			var parentCommit git.ObjectID
			var parentIds []string
			for i, step := range tc.steps {
				message := fmt.Sprintf("commit %d", i+1)
				commitID, err := executor.Commit(ctx, repo, CommitCommand{
					Repository: repoPath,
					Author:     author,
					Committer:  committer,
					Message:    message,
					Parent:     parentCommit.String(),
					Actions:    step.actions,
				})

				if step.error != nil {
					require.True(t, errors.Is(err, step.error), "expected: %q, actual: %q", step.error, err)
					continue
				} else {
					require.NoError(t, err)
				}

				commit, err := repo.ReadCommit(ctx, commitID.Revision())
				require.NoError(t, err)

				require.Equal(t, parentIds, commit.ParentIds)
				require.Equal(t, gittest.DefaultCommitAuthor, commit.Author)
				require.Equal(t, gittest.DefaultCommitAuthor, commit.Committer)
				require.Equal(t, []byte(message), commit.Body)

				if step.testCommit != nil {
					step.testCommit(t, commitID)
				}

				gittest.RequireTree(t, cfg, repoPath, commitID.String(), step.treeEntries)
				parentCommit = commitID
				parentIds = []string{string(commitID)}
			}
		})
	}
}

func extractSignature(tb testing.TB, ctx context.Context, repo *localrepo.Repo, oid git.ObjectID) (string, string) {
	data, err := repo.ReadObject(ctx, oid)
	require.NoError(tb, err)

	const gpgSignaturePrefix = "gpgsig "
	var (
		gpgsig, dataWithoutGpgSig string
		inSignature               bool
	)

	lines := strings.Split(string(data), "\n")

	for i, line := range lines {
		if line == "" {
			dataWithoutGpgSig += "\n" + strings.Join(lines[i+1:], "\n")
			break
		}

		if strings.HasPrefix(line, gpgSignaturePrefix) {
			inSignature = true
			gpgsig += strings.TrimPrefix(line, gpgSignaturePrefix)
		} else if inSignature {
			gpgsig += "\n" + strings.TrimPrefix(line, " ")
		} else {
			dataWithoutGpgSig += line + "\n"
		}
	}

	return gpgsig, dataWithoutGpgSig
}

func defaultCommitAuthorSignature() Signature {
	return NewSignature(
		gittest.DefaultCommitterName,
		gittest.DefaultCommitterMail,
		gittest.DefaultCommitTime,
	)
}
