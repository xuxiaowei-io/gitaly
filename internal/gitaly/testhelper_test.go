package gitaly

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
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

// RequireReferences asserts that the actual state of the references in the repository match the expected.
func RequireReferences(tb testing.TB, ctx context.Context, repo *localrepo.Repo, expectedReferences []git.Reference) {
	tb.Helper()

	actualReferences, err := repo.GetReferences(ctx)
	require.NoError(tb, err)
	require.ElementsMatch(tb, expectedReferences, actualReferences)
}
