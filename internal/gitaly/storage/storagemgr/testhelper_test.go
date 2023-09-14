package storagemgr

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// RepositoryState describes the full asserted state of a repository.
type RepositoryState struct {
	// NotFound when set asserts the repository should not exist. When set
	// the repository's directory asserted to not exist and the other
	// assertions ignored.
	NotFound bool
	// DefaultBranch is the expected refname that HEAD points to.
	DefaultBranch git.ReferenceName
	// References are references expected to exist.
	References []git.Reference
	// CustomHooks is the expected state of the custom hooks.
	CustomHooks testhelper.DirectoryState
	// Objects are the objects that are expected to exist.
	Objects []git.ObjectID
}

// RequireRepositoryState asserts the given repository matches the expected state.
func RequireRepositoryState(tb testing.TB, ctx context.Context, cfg config.Cfg, repo *localrepo.Repo, expected RepositoryState) {
	tb.Helper()

	repoPath, err := repo.Path()
	if expected.NotFound {
		require.Equal(tb, storage.NewRepositoryNotFoundError(
			repo.GetStorageName(), repo.GetRelativePath(),
		), err)
		return
	}

	require.NoError(tb, err)

	headReference, err := repo.HeadReference(ctx)
	require.NoError(tb, err)

	actualReferences, err := repo.GetReferences(ctx)
	require.NoError(tb, err)

	expectedObjects := []git.ObjectID{}
	if expected.Objects != nil {
		expectedObjects = expected.Objects
	}

	actualObjects := gittest.ListObjects(tb, cfg, repoPath)

	sortObjects := func(objects []git.ObjectID) {
		sort.Slice(objects, func(i, j int) bool {
			return objects[i] < objects[j]
		})
	}

	sortObjects(expectedObjects)
	sortObjects(actualObjects)

	require.Equal(tb,
		RepositoryState{
			DefaultBranch: expected.DefaultBranch,
			References:    expected.References,
			Objects:       expectedObjects,
		},
		RepositoryState{
			DefaultBranch: headReference,
			References:    actualReferences,
			Objects:       actualObjects,
		},
	)
	testhelper.RequireDirectoryState(tb, filepath.Join(repoPath, repoutil.CustomHooksDir), "", expected.CustomHooks)
}

type repositoryBuilder func(relativePath string) *localrepo.Repo

// RepositoryStates describes the state of repositories in a storage. The key is the relative path of a repository that
// is expected to exist and the value the expected state contained in the repository.
type RepositoryStates map[string]RepositoryState

// RequireRepositories finds all Git repositories in the given storage path. It asserts the set of existing repositories match the expected set
// and that all of the repositories contain the expected state.
func RequireRepositories(tb testing.TB, ctx context.Context, cfg config.Cfg, storagePath string, buildRepository repositoryBuilder, expected RepositoryStates) {
	tb.Helper()

	var actualRelativePaths []string
	require.NoError(tb, filepath.WalkDir(storagePath, func(path string, d fs.DirEntry, err error) error {
		require.NoError(tb, err)

		if !d.IsDir() {
			return nil
		}

		if err := storage.ValidateGitDirectory(path); err != nil {
			require.ErrorAs(tb, err, &storage.InvalidGitDirectoryError{})
			return nil
		}

		relativePath, err := filepath.Rel(storagePath, path)
		require.NoError(tb, err)

		actualRelativePaths = append(actualRelativePaths, relativePath)
		return nil
	}))

	var expectedRelativePaths []string
	for relativePath := range expected {
		expectedRelativePaths = append(expectedRelativePaths, relativePath)
	}

	// Sort the slices instead of using ElementsMatch so the state assertions are always done in the
	// same order as well.
	sort.Strings(actualRelativePaths)
	sort.Strings(expectedRelativePaths)

	require.Equal(tb, expectedRelativePaths, actualRelativePaths,
		"expected and actual set of repositories in the storage don't match")

	for _, relativePath := range expectedRelativePaths {
		func() {
			defer func() {
				// RequireRepositoryState works within a repository and doesn't thus print out the
				// relative path of the repository that failed. If the call failed the test,
				// print out the relative path to ease troubleshooting.
				if tb.Failed() {
					require.Failf(tb, "unexpected repository state", "relative path: %q", relativePath)
				}
			}()

			RequireRepositoryState(tb, ctx, cfg, buildRepository(relativePath), expected[relativePath])
		}()
	}
}

// DatabaseState describes the expected state of the key-value store. The keys in the map are the expected keys
// in the database and the values are the expected unmarshaled values.
type DatabaseState map[string]proto.Message

// RequireDatabase asserts the actual database state matches the expected database state. The actual values in the
// database are unmarshaled to the same type the values have in the expected database state.
func RequireDatabase(tb testing.TB, ctx context.Context, database *badger.DB, expectedState DatabaseState) {
	tb.Helper()

	if expectedState == nil {
		expectedState = DatabaseState{}
	}

	actualState := DatabaseState{}
	unexpectedKeys := []string{}
	require.NoError(tb, database.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			key := iterator.Item().Key()
			expectedValue, ok := expectedState[string(key)]
			if !ok {
				// Print the keys out escaped as otherwise the non-printing characters are not visible in the assertion failure.
				unexpectedKeys = append(unexpectedKeys, fmt.Sprintf("%q", key))
				continue
			}

			require.NoError(tb, iterator.Item().Value(func(value []byte) error {
				// Unmarshal the actual value to the same type as the expected value.
				actualValue := reflect.New(reflect.TypeOf(expectedValue).Elem()).Interface().(proto.Message)
				require.NoError(tb, proto.Unmarshal(value, actualValue))
				actualState[string(key)] = actualValue
				return nil
			}))
		}

		return nil
	}))

	require.Empty(tb, unexpectedKeys, "database contains unexpected keys")
	testhelper.ProtoEqual(tb, expectedState, actualState)
}
