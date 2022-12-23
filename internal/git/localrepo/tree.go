package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

// WriteTree writes a new tree object to the given path. This function does not verify whether OIDs
// referred to by tree entries actually exist in the repository.
func (r *Repo) WriteTree(ctx context.Context, entries []git.TreeEntry) (git.ObjectID, error) {
	var tree bytes.Buffer
	for _, entry := range entries {
		var entryType string

		switch entry.Mode {
		case "100644":
			entryType = "blob"
		case "100755":
			entryType = "blob"
		case "040000":
			entryType = "tree"
		case "160000":
			entryType = "commit"
		}

		oid := entry.OID

		formattedEntry := fmt.Sprintf("%s %s %s\t%s\000", string(entry.Mode), entryType, oid.String(), entry.Path)
		if _, err := tree.WriteString(formattedEntry); err != nil {
			return "", err
		}
	}

	options := []git.Option{
		git.Flag{Name: "-z"},
		git.Flag{Name: "--missing"},
	}

	var stdout, stderr bytes.Buffer
	if err := r.ExecAndWait(ctx,
		git.Command{
			Name:  "mktree",
			Flags: options,
		},
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
		git.WithStdin(&tree),
	); err != nil {
		return "", err
	}

	treeOID, err := git.ObjectHashSHA1.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", err
	}

	return treeOID, nil
}

func WriteTestTree(tb testing.TB, repo *Repo, entries []git.TreeEntry) git.ObjectID {
	oid, err := repo.WriteTree(testhelper.Context(tb), entries)
	require.NoError(tb, err)

	return oid
}
