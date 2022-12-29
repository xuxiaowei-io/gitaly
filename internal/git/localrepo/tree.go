package localrepo

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
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
			fallthrough
		case "100755":
			fallthrough
		case "120000":
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
