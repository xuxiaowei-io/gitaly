package praefect

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNewFeatureFlaggedRouter(t *testing.T) {
	ctx := testhelper.Context(t)
	flag := featureflag.FeatureFlag{Name: "test_flag"}

	enabledConn, disabledConn, enabledRouter, disabledRouter := setupFeatureFlaggedRouters(t, ctx)
	router := NewFeatureFlaggedRouter(flag, enabledRouter, disabledRouter)

	for _, tc := range []struct {
		desc         string
		flagEnabled  bool
		expectedConn *grpc.ClientConn
	}{
		{
			desc:         "flag disabled",
			flagEnabled:  false,
			expectedConn: disabledConn,
		},
		{
			desc:         "flag enabled",
			flagEnabled:  true,
			expectedConn: enabledConn,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx = featureflag.ContextWithFeatureFlag(ctx, flag, tc.flagEnabled)

			t.Run("RouteStorageAccessor", func(t *testing.T) {
				route, err := router.RouteStorageAccessor(ctx, "virtual-storage")
				require.NoError(t, err)
				require.Equal(t, tc.expectedConn.Target(), route.Connection.Target())
			})

			t.Run("RouteStorageMutator", func(t *testing.T) {
				_, err := router.RouteStorageMutator(ctx, "virtual-storage")
				require.Equal(t, errors.New("RouteStorageMutator is not implemented on PerRepositoryRouter"), err)
			})

			t.Run("RouteRepositoryAccessor", func(t *testing.T) {
				route, err := router.RouteRepositoryAccessor(ctx, "virtual-storage", "relative-path", false)
				require.NoError(t, err)
				require.Equal(t, tc.expectedConn.Target(), route.Node.Connection.Target())
			})

			t.Run("RouteRepositoryMutator", func(t *testing.T) {
				route, err := router.RouteRepositoryMutator(ctx, "virtual-storage", "relative-path", "")
				require.NoError(t, err)
				require.Equal(t, tc.expectedConn.Target(), route.Primary.Connection.Target())
			})

			t.Run("RouteRepositoryCreation", func(t *testing.T) {
				route, err := router.RouteRepositoryCreation(ctx, "virtual-storage", "relative-path-new", "")
				require.NoError(t, err)
				require.Equal(t, tc.expectedConn.Target(), route.Primary.Connection.Target())
			})

			t.Run("RouteRepositoryMaintenance", func(t *testing.T) {
				route, err := router.RouteRepositoryMaintenance(ctx, "virtual-storage", "relative-path")
				require.NoError(t, err)
				require.Equal(t, tc.expectedConn.Target(), route.Nodes[0].Connection.Target())
			})
		})
	}
}

func setupFeatureFlaggedRouters(t *testing.T, ctx context.Context) (*grpc.ClientConn, *grpc.ClientConn, *PerRepositoryRouter, *PerRepositoryRouter) {
	db := testdb.New(t)
	tx := db.Begin(t)
	t.Cleanup(func() { tx.Rollback(t) })
	configuredStorages := map[string][]string{"virtual-storage": {"storage"}}
	testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect": configuredStorages})

	repositoryStore := datastore.NewPostgresRepositoryStore(tx, configuredStorages)
	require.NoError(t, repositoryStore.CreateRepository(
		ctx,
		1,
		"virtual-storage",
		"relative-path",
		"replica-path",
		"storage",
		nil,
		nil,
		true,
		true,
	))

	// We don't need real connections, just the target in order to assert that we got the right connection back
	// from the router. We mock out the actual connection with a net.Pipe.
	enabledConn, err := grpc.Dial("enabledConn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		_, pw := net.Pipe()
		return pw, nil
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	// Close the connection immediately to clean up as we just care about the value of .Target() in the test.
	testhelper.MustClose(t, enabledConn)

	disabledConn, err := grpc.Dial("disabledConn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		_, pw := net.Pipe()
		return pw, nil
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	testhelper.MustClose(t, disabledConn)

	elector := nodes.NewPerRepositoryElector(tx)
	healthChecker := StaticHealthChecker{"virtual-storage": {"storage"}}
	assignmentStore := datastore.NewAssignmentStore(tx, configuredStorages)
	randomGenerator := rand.New(rand.NewSource(1))

	enabledRouter := NewPerRepositoryRouter(
		Connections{"virtual-storage": {"storage": enabledConn}},
		elector,
		healthChecker,
		randomGenerator,
		repositoryStore,
		assignmentStore,
		repositoryStore,
		nil,
	)

	disabledRouter := NewPerRepositoryRouter(
		Connections{"virtual-storage": {"storage": disabledConn}},
		elector,
		healthChecker,
		randomGenerator,
		repositoryStore,
		assignmentStore,
		repositoryStore,
		nil,
	)

	return enabledConn, disabledConn, enabledRouter, disabledRouter
}
