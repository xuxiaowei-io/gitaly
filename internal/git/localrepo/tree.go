package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
)

// ObjectType is an Enum for the type of object of
// the ls-tree entry, which can be can be tree, blob or commit
type ObjectType int

// Entries holds every ls-tree Entry
type Entries []TreeEntry

// Enum values for ObjectType
const (
	Unknown ObjectType = iota
	Tree
	Blob
	Submodule
)

func (e Entries) Len() int {
	return len(e)
}

func (e Entries) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// Less sorts entries by type in the order [*tree *blobs *submodules]
func (e Entries) Less(i, j int) bool {
	return e[i].Type < e[j].Type
}

// ByPath allows a slice of *TreeEntry to be sorted by Path
type ByPath []*TreeEntry

func (b ByPath) Len() int {
	return len(b)
}

func (b ByPath) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

// ToEnum translates a string representation of the object type into an
// ObjectType enum.
func ToEnum(s string) ObjectType {
	switch s {
	case "tree":
		return Tree
	case "blob":
		return Blob
	case "commit":
		return Submodule
	default:
		return Unknown
	}
}

// TreeEntry represents an entry of a git tree object.
type TreeEntry struct {
	// OID is the object ID the tree entry refers to.
	OID git.ObjectID
	// Mode is the file mode of the tree entry.
	Mode string
	// Path is the full path of the tree entry.
	Path string
	// Type is the type of the tree entry.
	Type ObjectType

	Entries []*TreeEntry
}

// IsBlob returns whether or not the TreeEntry is a blob.
func (t *TreeEntry) IsBlob() bool {
	return t.Type == Blob
}

// WriteTree writes a new tree object to the given path. This function does not verify whether OIDs
// referred to by tree entries actually exist in the repository.
func (repo *Repo) WriteTree(ctx context.Context, entries []*TreeEntry) (git.ObjectID, error) {
	var tree bytes.Buffer

	sort.Stable(ByPath(entries))

	for _, entry := range entries {
		mode := strings.TrimPrefix(entry.Mode, "0")
		formattedEntry := fmt.Sprintf("%s %s\000", mode, entry.Path)

		oidBytes, err := entry.OID.Bytes()
		if err != nil {
			return "", err
		}

		if _, err := tree.WriteString(formattedEntry); err != nil {
			return "", err
		}

		if _, err := tree.Write(oidBytes); err != nil {
			return "", err
		}
	}

	options := []git.Option{
		git.ValueFlag{Name: "-t", Value: "tree"},
		git.Flag{Name: "-w"},
		git.Flag{Name: "--stdin"},
	}

	var stdout, stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name:  "hash-object",
			Flags: options,
		},
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
		git.WithStdin(&tree),
	); err != nil {
		return "", fmt.Errorf("%w: %s", err, stderr.String())
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

func depthByPath(path string) int {
	return len(strings.Split(path, "/"))
}

type treeQueue []*TreeEntry

func (t treeQueue) first() *TreeEntry {
	return t[len(t)-1]
}

func (t *treeQueue) push(e *TreeEntry) {
	*t = append(*t, e)
}

func (t *treeQueue) pop() *TreeEntry {
	e := t.first()

	*t = (*t)[:len(*t)-1]

	return e
}

// GetFullTree gets a tree object with all of the Entries populated for every
// level of the tree.
func (repo *Repo) GetFullTree(ctx context.Context, treeish git.Revision) (*TreeEntry, error) {
	treeOID, err := repo.ResolveRevision(ctx, git.Revision(fmt.Sprintf("%s^{tree}", treeish)))
	if err != nil {
		return nil, fmt.Errorf("getting revision %w", err)
	}

	rootEntry := &TreeEntry{
		OID:  treeOID,
		Type: Tree,
		Mode: "040000",
	}

	entries, err := repo.ListEntries(
		ctx,
		git.Revision(treeOID),
		&ListEntriesConfig{
			Recursive: true,
		},
	)

	queue := treeQueue{rootEntry}

	for _, entry := range entries {
		if depthByPath(entry.Path) < len(queue) {
			levelsToPop := len(queue) - depthByPath(entry.Path)

			for i := 0; i < levelsToPop; i++ {
				queue.pop()
			}
		}

		entry.Path = filepath.Base(entry.Path)
		queue.first().Entries = append(
			queue.first().Entries,
			entry,
		)

		if entry.Type == Tree {
			queue.push(entry)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("listing entries: %w", err)
	}

	return rootEntry, nil
}
