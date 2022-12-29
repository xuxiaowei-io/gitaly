package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

// TreeEntry represents an entry of a git tree object.
type TreeEntry struct {
	// OID is the object ID the tree entry refers to.
	OID git.ObjectID
	// Mode is the file mode of the tree entry.
	Mode string
	// Path is the full path of the tree entry.
	Path string
	// Content is the content of the tree entry.
	Content string
}

// WriteTree writes a new tree object to the given path. This function does not verify whether OIDs
// referred to by tree entries actually exist in the repository.
func (repo *Repo) WriteTree(ctx context.Context, entries []TreeEntry) (git.ObjectID, error) {
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

		formattedEntry := fmt.Sprintf("%s %s %s\t%s\000", entry.Mode, entryType, oid.String(), entry.Path)
		if _, err := tree.WriteString(formattedEntry); err != nil {
			return "", err
		}
	}

	options := []git.Option{
		git.Flag{Name: "-z"},
		git.Flag{Name: "--missing"},
	}

	var stdout, stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
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

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("detecting object hash: %w", err)
	}

	treeOID, err := objectHash.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", err
	}

	return treeOID, nil
}

// WriteTestTree can be used to write trees into repositories in tests.
// Prefer this function over the one in gittest.
func WriteTestTree(tb testing.TB, repo *Repo, entries []TreeEntry) git.ObjectID {
	ctx := testhelper.Context(tb)
	for i, entry := range entries {
		if entry.OID == "" {
			oid, err := repo.WriteBlob(ctx, entry.Path, strings.NewReader(entry.Content))
			require.NoError(tb, err)
			entries[i].OID = oid
		}
	}

	oid, err := repo.WriteTree(ctx, entries)
	require.NoError(tb, err)

	return oid
}
