package storagemgr

import (
	"context"
	"fmt"
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
