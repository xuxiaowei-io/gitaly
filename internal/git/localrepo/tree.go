package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
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

func fromEnum(t ObjectType) string {
	switch t {
	case Tree:
		return "tree"
	case Blob:
		return "blob"
	case Submodule:
		return "commit"
	default:
		return "unknown"
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

var (
	// ErrTreeNotFound indicates the tree in question was not found.
	ErrTreeNotFound = errors.New("tree not found")
	// ErrEntryNotFound indicates an entry could not be found.
	ErrEntryNotFound = errors.New("entry not found")
	// ErrEntryExists indicates an entry already exists.
	ErrEntryExists = errors.New("entry already exists")
	// ErrDirExists indicates a directory already exists.
	ErrDirExists = errors.New("directory already exists")
	// ErrPathTraversal indicates a path contains a traversal.
	ErrPathTraversal = errors.New("path contains traversal")
	// ErrInvalidPath indicates a path is invalid.
	ErrInvalidPath = errors.New("invalid path")
)

func validateFileCreationPath(path string) error {
	if filepath.Clean(path) != path {
		return ErrPathTraversal
	}

	if strings.HasPrefix(path, ".git") {
		return ErrInvalidPath
	}

	return nil
}

// Add takes an entry and adds it to an existing TreeEntry
// based on the path. Call WriteTreeRecursively to write it
// to the Git object database.
func (t *TreeEntry) Add(
	ctx context.Context,
	path string,
	newEntry TreeEntry,
	overwriteFile, overwriteDir bool,
) error {
	if err := validateFileCreationPath(path); err != nil {
		return err
	}

	paths := strings.Split(path, "/")

	currentEntry := t

	for _, subPath := range paths {
		if len(currentEntry.Entries) == 0 {
			if subPath == filepath.Base(path) {
				currentEntry.Entries = []*TreeEntry{
					&newEntry,
				}
			} else {
				currentEntry.Entries = []*TreeEntry{
					{
						Mode: "040000",
						Type: Tree,
						Path: subPath,
					},
				}
			}

			currentEntry.OID = ""
			currentEntry = currentEntry.Entries[0]
			continue
		}

		for i, entry := range currentEntry.Entries {
			if entry.Path != subPath {
				continue
			}

			if entry.Path == filepath.Base(path) {
				if entry.Type == Tree && !overwriteDir ||
					entry.Type == Blob && !overwriteFile {
					return errors.New("cannot overwrite entry")
				}

				currentEntry.Entries[i] = &newEntry
				currentEntry.OID = ""

				t.markTreesForWrite(path)

				return nil
			}

			currentEntry.OID = ""
			currentEntry = entry
			break
		}

		if subPath == filepath.Base(path) {
			currentEntry.Entries = append(
				currentEntry.Entries,
				&newEntry,
			)
			currentEntry.OID = ""

			t.markTreesForWrite(path)

			return nil
		}
	}

	return nil
}

func (t *TreeEntry) markTreesForWrite(
	path string,
) {
	paths := strings.Split(path, "/")

	currentEntry := t

	for _, subPath := range paths {
		for _, entry := range currentEntry.Entries {
			if subPath != entry.Path {
				continue
			}

			currentEntry.OID = ""

			if filepath.Base(path) == entry.Path {
				return
			}

			currentEntry = entry
		}
	}
}

// Delete deletes the entry of a current tree based on the path.
func (t *TreeEntry) Delete(
	ctx context.Context,
	path string,
) error {
	if err := validateFileCreationPath(path); err != nil {
		return err
	}

	paths := strings.Split(path, "/")

	currentEntry := t

	for _, subPath := range paths {
		for i, entry := range currentEntry.Entries {
			if subPath != entry.Path {
				continue
			}

			if filepath.Base(path) == entry.Path {
				t.markTreesForWrite(path)

				currentEntry.Entries = append(
					currentEntry.Entries[:i],
					currentEntry.Entries[i+1:]...)

				return nil
			}

			currentEntry = entry
		}
	}

	return ErrEntryNotFound
}

// Modify modifies an existing TreeEntry based on a path and a function to
// modify an entry.
func (t *TreeEntry) Modify(
	ctx context.Context,
	path string,
	modifyEntry func(*TreeEntry) error,
) error {
	if err := validateFileCreationPath(path); err != nil {
		return err
	}

	paths := strings.Split(path, "/")

	currentEntry := t

	for _, subPath := range paths {
		for _, entry := range currentEntry.Entries {
			if subPath != entry.Path {
				continue
			}

			if filepath.Base(path) == entry.Path {
				t.markTreesForWrite(path)

				return modifyEntry(entry)
			}

			currentEntry = entry
		}
	}

	return ErrEntryNotFound
}

// WriteTreeRecursively takes a TreeEntry, and does a depth first walk, writing
// new trees when needed.
func (repo *Repo) WriteTreeRecursively(
	ctx context.Context,
	entry *TreeEntry,
) (git.ObjectID, error) {
	var err error

	if entry.OID != "" {
		return entry.OID, nil
	}

	for _, e := range entry.Entries {
		if e.Type == Tree && e.OID == "" {
			e.OID, err = repo.WriteTreeRecursively(ctx, e)
			if err != nil {
				return "", err
			}
		}
	}

	treeOID, err := repo.WriteTree(ctx, entry.Entries)
	if err != nil {
		return "", err
	}

	return treeOID, nil
}

// WriteTree writes a new tree object to the given path. This function does not verify whether OIDs
// referred to by tree entries actually exist in the repository.
func (repo *Repo) WriteTree(ctx context.Context, entries []*TreeEntry) (git.ObjectID, error) {
	var tree bytes.Buffer
	for _, entry := range entries {
		entryType := entry.Type

		if entryType == Unknown {
			switch entry.Mode {
			case "100644":
				fallthrough
			case "100755":
				fallthrough
			case "120000":
				entryType = Blob
			case "040000":
				entryType = Tree
			case "160000":
				entryType = Submodule
			}
		}

		oid := entry.OID

		formattedEntry := fmt.Sprintf("%s %s %s\t%s\000", entry.Mode, fromEnum(entryType), oid.String(), entry.Path)
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

func (repo *Repo) walkTree(ctx context.Context, root *TreeEntry) error {
	entries, err := repo.ListEntries(
		ctx,
		git.Revision(root.OID),
		&ListEntriesConfig{},
	)
	if err != nil {
		return err
	}

	for i := range entries {
		if entries[i].Type == Tree {
			if err := repo.walkTree(ctx, entries[i]); err != nil {
				return err
			}
		}
	}

	root.Entries = entries

	return nil
}

// GetFullTree gets a tree object with all of the entries filled out for every
// level.
func (repo *Repo) GetFullTree(ctx context.Context, treeish git.Revision) (TreeEntry, error) {
	treeOID, err := repo.ResolveRevision(ctx, git.Revision(fmt.Sprintf("%s^{tree}", treeish)))
	if err != nil {
		return TreeEntry{}, err
	}

	rootEntry := TreeEntry{
		OID:  treeOID,
		Type: Tree,
		Mode: "040000",
	}

	if err := repo.walkTree(ctx, &rootEntry); err != nil {
		return TreeEntry{}, err
	}

	return rootEntry, nil
}
