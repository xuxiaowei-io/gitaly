//go:build !gitaly_test_sha256

package namespace

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestNamespaceExists(t *testing.T) {
	cfg, client := setupNamespaceService(t, testserver.WithDisablePraefect())
	existingStorage := cfg.Storages[0]
	ctx := testhelper.Context(t)

	const existingNamespace = "existing"
	require.NoError(t, os.MkdirAll(filepath.Join(existingStorage.Path, existingNamespace), perm.SharedDir))

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.NamespaceExistsRequest
		expectedResponse *gitalypb.NamespaceExistsResponse
		expectedErr      error
	}{
		{
			desc: "empty name",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage.Name,
				Name:        "",
			},
			expectedErr: structerr.NewInvalidArgument("Name: cannot be empty"),
		},
		{
			desc: "Namespace doesn't exists",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage.Name,
				Name:        "not-existing",
			},
			expectedResponse: &gitalypb.NamespaceExistsResponse{
				Exists: false,
			},
		},
		{
			desc: "Wrong storage path",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: "other",
				Name:        existingNamespace,
			},
			expectedResponse: &gitalypb.NamespaceExistsResponse{
				Exists: false,
			},
		},
		{
			desc: "Namespace exists",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage.Name,
				Name:        existingNamespace,
			},
			expectedResponse: &gitalypb.NamespaceExistsResponse{
				Exists: true,
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := client.NamespaceExists(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}

func getStorageDir(t *testing.T, cfg config.Cfg, storageName string) string {
	t.Helper()
	s, found := cfg.Storage(storageName)
	require.True(t, found)
	return s.Path
}

func TestAddNamespace(t *testing.T) {
	testhelper.SkipWithPraefect(t, "per_repository election strategy doesn't support storage scoped mutators")

	ctx := testhelper.Context(t)
	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.AddNamespaceRequest
		expectedErr error
	}{
		{
			desc: "No name",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        "",
			},
			expectedErr: structerr.NewInvalidArgument("Name: cannot be empty"),
		},
		{
			desc: "Namespace is successfully created",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        "create-me",
			},
		},
		{
			desc: "Idempotent on creation",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        "create-me",
			},
		},
		{
			desc: "no storage",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: "",
				Name:        "mepmep",
			},
			expectedErr: structerr.NewInvalidArgument("GetStorageByName: no such storage: %q", ""),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			_, err := client.AddNamespace(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			if tc.expectedErr == nil {
				require.DirExists(t, filepath.Join(existingStorage.Path, tc.request.Name))
			}
		})
	}
}

func TestRemoveNamespace(t *testing.T) {
	testhelper.SkipWithPraefect(t, "per_repository election strategy doesn't support storage scoped mutators")

	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]
	ctx := testhelper.Context(t)

	const existingNamespace = "created"
	require.NoError(t, os.MkdirAll(filepath.Join(existingStorage.Path, existingNamespace), perm.SharedDir), "test setup")

	queries := []struct {
		desc        string
		request     *gitalypb.RemoveNamespaceRequest
		expectedErr error
	}{
		{
			desc: "Namespace is successfully removed",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        existingNamespace,
			},
		},
		{
			desc: "Idempotent on deletion",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        "not-there",
			},
		},
		{
			desc: "no storage",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: "",
				Name:        "mepmep",
			},
			expectedErr: structerr.NewInvalidArgument("GetStorageByName: no such storage: %q", ""),
		},
	}

	for _, tc := range queries {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			_, err := client.RemoveNamespace(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			if tc.expectedErr == nil {
				require.NoFileExists(t, filepath.Join(existingStorage.Path, tc.request.Name))
			}
		})
	}
}

func TestRenameNamespace(t *testing.T) {
	testhelper.SkipWithPraefect(t, "per_repository election strategy doesn't support storage scoped mutators")

	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]
	ctx := testhelper.Context(t)

	const existingNamespace = "existing"
	require.NoError(t, os.MkdirAll(filepath.Join(existingStorage.Path, existingNamespace), perm.SharedDir))

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.RenameNamespaceRequest
		expectedErr error
	}{
		{
			desc: "Renaming an existing namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        existingNamespace,
				To:          "new-path",
				StorageName: existingStorage.Name,
			},
		},
		{
			desc: "No from given",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "",
				To:          "new-path",
				StorageName: existingStorage.Name,
			},
			expectedErr: structerr.NewInvalidArgument("from and to cannot be empty"),
		},
		{
			desc: "non-existing namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "non-existing",
				To:          "new-path",
				StorageName: existingStorage.Name,
			},
			expectedErr: structerr.NewInvalidArgument("to directory new-path already exists"),
		},
		{
			desc: "existing destination namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        existingNamespace,
				To:          existingNamespace,
				StorageName: existingStorage.Name,
			},
			expectedErr: structerr.NewInvalidArgument("from directory existing not found"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameNamespace(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			if tc.expectedErr == nil {
				require.DirExists(t, filepath.Join(existingStorage.Path, tc.request.To))
			}
		})
	}
}

func TestRenameNamespaceWithNonexistentParentDir(t *testing.T) {
	testhelper.SkipWithPraefect(t, "per_repository election strategy doesn't support storage scoped mutators")

	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]
	ctx := testhelper.Context(t)

	_, err := client.AddNamespace(ctx, &gitalypb.AddNamespaceRequest{
		StorageName: existingStorage.Name,
		Name:        "existing",
	})
	require.NoError(t, err)

	_, err = client.RenameNamespace(ctx, &gitalypb.RenameNamespaceRequest{
		From:        "existing",
		To:          "some/other/new-path",
		StorageName: existingStorage.Name,
	})
	require.NoError(t, err)

	storagePath := getStorageDir(t, cfg, existingStorage.Name)
	require.NoError(t, err)
	toDir := namespacePath(storagePath, "some/other/new-path")
	require.DirExists(t, toDir)
}
