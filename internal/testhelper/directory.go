package testhelper

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// DirectoryEntry models an entry in a directory.
type DirectoryEntry struct {
	// Mode is the file mode of the entry.
	Mode fs.FileMode
	// Content contains the file content if this is a regular file.
	Content []byte
}

// DirectoryState models the contents of a directory. The key is relative of the entry in
// the rootDirectory as described on RequireDirectoryState.
type DirectoryState map[string]DirectoryEntry

// RequireDirectoryState asserts that given directory matches the expected state. The rootDirectory and
// relativeDirectory are joined together to decide the directory to walk. rootDirectory is trimmed out of the
// paths in the DirectoryState to make assertions easier by using the relative paths only. For example, given
// `/root-path` and `relative/path`, the directory walked is `/root-path/relative/path`. The paths in DirectoryState
// trim the root prefix, thus they should be like `/relative/path/...`. The beginning point of the walk has path "/".
func RequireDirectoryState(tb testing.TB, rootDirectory, relativeDirectory string, expected DirectoryState) {
	tb.Helper()

	actual := DirectoryState{}
	require.NoError(tb, filepath.WalkDir(filepath.Join(rootDirectory, relativeDirectory), func(path string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(tb, err)

		trimmedPath := strings.TrimPrefix(path, rootDirectory)
		if trimmedPath == "" {
			// Store the walked directory itself as "/". Less confusing than having it be
			// an emptry string.
			trimmedPath = string(os.PathSeparator)
		}

		info, err := entry.Info()
		require.NoError(tb, err)

		actualEntry := DirectoryEntry{
			Mode: info.Mode(),
		}

		if entry.Type().IsRegular() {
			actualEntry.Content, err = os.ReadFile(path)
			require.NoError(tb, err)
		}

		actual[trimmedPath] = actualEntry

		return nil
	}))

	if expected == nil {
		expected = DirectoryState{}
	}

	require.Equal(tb, expected, actual)
}
