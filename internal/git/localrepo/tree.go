package localrepo

import (
	"bytes"
	"context"
	"errors"
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

var (
	// ErrEntryNotFound indicates an entry could not be found.
	ErrEntryNotFound = errors.New("entry not found")
	// ErrEntryExists indicates an entry already exists.
	ErrEntryExists = errors.New("entry already exists")
	// ErrPathTraversal indicates a path contains a traversal.
	ErrPathTraversal = errors.New("path contains traversal")
)

func validateFileCreationPath(path string) error {
	if filepath.Clean(path) != path {
		return ErrPathTraversal
	}

	return nil
}

type addTreeEntryConfig struct {
	overwriteFile bool
	overwriteDir  bool
}

// AddTreeEntryOption is a function that sets options on the addTreeEntryConfig
// struct.
type AddTreeEntryOption func(*addTreeEntryConfig)

// WithOverwriteFile allows Add to overwrite a file
func WithOverwriteFile() AddTreeEntryOption {
	return func(a *addTreeEntryConfig) {
		a.overwriteFile = true
	}
}

// WithOverwriteDirectory allows Add to overwrite a directory
func WithOverwriteDirectory() AddTreeEntryOption {
	return func(a *addTreeEntryConfig) {
		a.overwriteDir = true
	}
}

// Add takes an entry and adds it to an existing TreeEntry
// based on path.
// It works by walking down the TreeEntry's Entries, layer by layer, based on
// path. If it reaches the limit when walking down the tree, that means the
// rest of the directories path will need to be created.
// If we're able to walk all the way down the tree based on path, then we
// either add the new entry to the last subtree's entries if it doesn't yet
// exist, or optionally overwrite the entry if it already exists.
func (t *TreeEntry) Add(
	path string,
	newEntry TreeEntry,
	options ...AddTreeEntryOption,
) error {
	if err := validateFileCreationPath(path); err != nil {
		return err
	}

	var cfg addTreeEntryConfig

	for _, option := range options {
		option(&cfg)
	}

	var subPath string
	var ok bool

	tail := path

	// currentTree keeps track of the current tree we are examining.
	currentTree := t
	for {
		// loop through each layer of the tree based on the directory
		// structure specified by path.
		// subPath is the current directory name, and tail is the rest
		// of the sub directories we have yet to walk down into.
		subPath, tail, ok = strings.Cut(tail, "/")
		if !ok {
			if subPath == "" {
				break
			}
		}

		var recurse bool

		// look through the current tree's entries.
		for i, entry := range currentTree.Entries {
			// if the entry's Path does not match subPath, we don't
			// want to do anything with it.
			if entry.Path != subPath {
				continue
			}

			// if the entry's Path does match subPath, then either
			// we found the next directory to walk into, or we've
			// found the entry we want to replace.

			// we found an entry in the tree that matches the entry
			// we want to add. Replace the entry or throw an error,
			// depending on the options passed in via options
			if entry.Path == filepath.Base(path) {
				if (entry.Type == Tree && !cfg.overwriteDir) ||
					(entry.Type == Blob && !cfg.overwriteFile) {
					return ErrEntryExists
				}

				currentTree.OID = ""
				currentTree.Entries[i] = &newEntry

				return nil
			}

			// we found the next directory to walk into.
			recurse = true
			currentTree.OID = ""
			currentTree = entry
		}

		if recurse {
			continue
		}

		// if we get here, that means we didn't find any directories to
		// recurse into, which means we need to to create a brand new
		// tree
		if subPath == filepath.Base(path) {
			currentTree.OID = ""
			currentTree.Entries = append(
				currentTree.Entries,
				&newEntry,
			)

			return nil
		}

		currentTree.Entries = append(currentTree.Entries, &TreeEntry{
			Mode: "040000",
			Type: Tree,
			Path: subPath,
		})

		currentTree.OID = ""
		currentTree = currentTree.Entries[len(currentTree.Entries)-1]
	}

	return nil
}

// recurse is a common function to recurse down into a TreeEntry based on path,
// and take some action once we've found the entry in question.
func (t *TreeEntry) recurse(
	path string,
	fn func(currentTree, entry *TreeEntry, i int) error,
) error {
	var subPath string
	var ok bool
	tail := path

	currentTree := t
	for {
		// loop through each layer of the tree based on the directory
		// structure specified by path.
		// subPath is the current directory name, and tail is the rest
		// of the sub directories we have yet to walk down into.
		subPath, tail, ok = strings.Cut(tail, "/")
		if !ok {
			if subPath == "" {
				break
			}
		}
		// look through the current tree's entries.
		for i, entry := range currentTree.Entries {
			if subPath != entry.Path {
				continue
			}

			if filepath.Base(path) == entry.Path {
				currentTree.OID = ""

				// once we find the entry in question, apply the
				// function to modify the current tree and/or
				// entry.
				return fn(currentTree, entry, i)
			}

			currentTree.OID = ""
			currentTree = entry
		}
	}

	return ErrEntryNotFound
}

// Delete deletes the entry of a current tree based on the path.
func (t *TreeEntry) Delete(
	path string,
) error {
	if err := validateFileCreationPath(path); err != nil {
		return err
	}

	return t.recurse(path, func(currentTree, entry *TreeEntry, i int) error {
		currentTree.Entries = append(
			currentTree.Entries[:i],
			currentTree.Entries[i+1:]...)

		return nil
	})
}

// Modify modifies an existing TreeEntry based on a path and a function to
// modify an entry.
func (t *TreeEntry) Modify(
	path string,
	modifyEntry func(*TreeEntry) error,
) error {
	if err := validateFileCreationPath(path); err != nil {
		return err
	}

	return t.recurse(path, func(currentTree, entry *TreeEntry, _ int) error {
		return modifyEntry(entry)
	})
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

	if len(entry.Entries) == 0 {
		return "", errors.New("cannot write empty tree")
	}

	treeOID, err := repo.WriteTree(ctx, entry.Entries)
	if err != nil {
		return "", fmt.Errorf("writing tree: %w", err)
	}

	return treeOID, nil
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
