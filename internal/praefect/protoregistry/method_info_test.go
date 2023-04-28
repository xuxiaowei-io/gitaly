package protoregistry

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestMethodInfo_getRepo(t *testing.T) {
	t.Parallel()

	testRepos := []*gitalypb.Repository{
		{
			GitAlternateObjectDirectories: []string{"a", "b", "c"},
			GitObjectDirectory:            "d",
			GlProjectPath:                 "e",
			GlRepository:                  "f",
			RelativePath:                  "g",
			StorageName:                   "h",
		},
		{
			GitAlternateObjectDirectories: []string{"1", "2", "3"},
			GitObjectDirectory:            "4",
			GlProjectPath:                 "5",
			GlRepository:                  "6",
			RelativePath:                  "7",
			StorageName:                   "8",
		},
	}

	testcases := []struct {
		desc                 string
		svc                  string
		method               string
		pbMsg                proto.Message
		expectRepo           *gitalypb.Repository
		expectAdditionalRepo *gitalypb.Repository
		expectErr            error
	}{
		{
			desc:   "valid request type single depth",
			svc:    "RepositoryService",
			method: "OptimizeRepository",
			pbMsg: &gitalypb.OptimizeRepositoryRequest{
				Repository: testRepos[0],
			},
			expectRepo: testRepos[0],
		},
		{
			desc:   "target nested in oneOf",
			svc:    "OperationService",
			method: "UserCommitFiles",
			pbMsg: &gitalypb.UserCommitFilesRequest{
				UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Header{
					Header: &gitalypb.UserCommitFilesRequestHeader{
						Repository: testRepos[1],
					},
				},
			},
			expectRepo: testRepos[1],
		},
		{
			desc:   "target nested, includes additional repository",
			svc:    "ObjectPoolService",
			method: "FetchIntoObjectPool",
			pbMsg: &gitalypb.FetchIntoObjectPoolRequest{
				Origin:     testRepos[0],
				ObjectPool: &gitalypb.ObjectPool{Repository: testRepos[1]},
			},
			expectRepo:           testRepos[1],
			expectAdditionalRepo: testRepos[0],
		},
		{
			desc:      "target repo is nil",
			svc:       "RepositoryService",
			method:    "OptimizeRepository",
			pbMsg:     &gitalypb.OptimizeRepositoryRequest{Repository: nil},
			expectErr: ErrTargetRepoMissing,
		},
	}

	for _, tc := range testcases {
		desc := fmt.Sprintf("%s:%s %s", tc.svc, tc.method, tc.desc)
		t.Run(desc, func(t *testing.T) {
			info, err := GitalyProtoPreregistered.LookupMethod(fmt.Sprintf("/gitaly.%s/%s", tc.svc, tc.method))
			require.NoError(t, err)

			actualTarget, actualErr := info.TargetRepo(tc.pbMsg)
			require.Equal(t, tc.expectErr, actualErr)

			// not only do we want the value to be the same, but we actually want the
			// exact same instance to be returned
			if tc.expectRepo != actualTarget {
				t.Fatal("pointers do not match")
			}

			if tc.expectAdditionalRepo != nil {
				additionalRepo, ok, err := info.AdditionalRepo(tc.pbMsg)
				require.True(t, ok)
				require.NoError(t, err)
				require.Equal(t, tc.expectAdditionalRepo, additionalRepo)
			}
		})
	}
}

func TestMethodInfo_Storage(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		desc          string
		svc           string
		method        string
		pbMsg         proto.Message
		expectStorage string
		expectErr     error
	}{
		{
			desc:   "valid request type single depth",
			svc:    "NamespaceService",
			method: "AddNamespace",
			pbMsg: &gitalypb.AddNamespaceRequest{
				StorageName: "some_storage",
			},
			expectStorage: "some_storage",
		},
		{
			desc:      "incorrect request type",
			svc:       "RepositoryService",
			method:    "OptimizeRepository",
			pbMsg:     &gitalypb.OptimizeRepositoryResponse{},
			expectErr: errors.New("proto message gitaly.OptimizeRepositoryResponse does not match expected RPC request message gitaly.OptimizeRepositoryRequest"),
		},
	}

	for _, tc := range testcases {
		desc := fmt.Sprintf("%s:%s %s", tc.svc, tc.method, tc.desc)
		t.Run(desc, func(t *testing.T) {
			info, err := GitalyProtoPreregistered.LookupMethod(fmt.Sprintf("/gitaly.%s/%s", tc.svc, tc.method))
			require.NoError(t, err)

			actualStorage, actualErr := info.Storage(tc.pbMsg)
			require.Equal(t, tc.expectErr, actualErr)

			// not only do we want the value to be the same, but we actually want the
			// exact same instance to be returned
			if tc.expectStorage != actualStorage {
				t.Fatal("pointers do not match")
			}
		})
	}
}

func TestMethodInfo_SetStorage(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc      string
		service   string
		method    string
		pbMsg     proto.Message
		storage   string
		expectErr error
	}{
		{
			desc:    "valid request type",
			service: "NamespaceService",
			method:  "AddNamespace",
			pbMsg: &gitalypb.AddNamespaceRequest{
				StorageName: "old_storage",
			},
			storage: "new_storage",
		},
		{
			desc:      "incorrect request type",
			service:   "RepositoryService",
			method:    "OptimizeRepository",
			pbMsg:     &gitalypb.OptimizeRepositoryResponse{},
			expectErr: errors.New("proto message gitaly.OptimizeRepositoryResponse does not match expected RPC request message gitaly.OptimizeRepositoryRequest"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			info, err := GitalyProtoPreregistered.LookupMethod("/gitaly." + tc.service + "/" + tc.method)
			require.NoError(t, err)

			err = info.SetStorage(tc.pbMsg, tc.storage)
			if tc.expectErr == nil {
				require.NoError(t, err)
				changed, err := info.Storage(tc.pbMsg)
				require.NoError(t, err)
				require.Equal(t, tc.storage, changed)
			} else {
				require.Equal(t, tc.expectErr, err)
			}
		})
	}
}

func TestMethodInfo_RequestFactory(t *testing.T) {
	t.Parallel()

	mInfo, err := GitalyProtoPreregistered.LookupMethod("/gitaly.RepositoryService/RepositoryExists")
	require.NoError(t, err)

	pb, err := mInfo.UnmarshalRequestProto([]byte{})
	require.NoError(t, err)

	testhelper.ProtoEqual(t, &gitalypb.RepositoryExistsRequest{}, pb)
}

func TestMethodInfoScope(t *testing.T) {
	for _, tt := range []struct {
		method string
		scope  Scope
	}{
		{
			method: "/gitaly.RepositoryService/RepositoryExists",
			scope:  ScopeRepository,
		},
	} {
		t.Run(tt.method, func(t *testing.T) {
			mInfo, err := GitalyProtoPreregistered.LookupMethod(tt.method)
			require.NoError(t, err)

			require.Exactly(t, tt.scope, mInfo.Scope)
		})
	}
}

func BenchmarkMethodInfo(b *testing.B) {
	for _, bc := range []struct {
		desc        string
		method      string
		request     proto.Message
		expectedErr error
	}{
		{
			desc:   "unset target repository",
			method: "/gitaly.RepositoryService/OptimizeRepository",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: nil,
			},
			expectedErr: ErrTargetRepoMissing,
		},
		{
			desc:   "target repository",
			method: "/gitaly.RepositoryService/OptimizeRepository",
			request: &gitalypb.OptimizeRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "something",
					RelativePath: "something",
				},
			},
		},
		{
			desc:   "target object pool",
			method: "/gitaly.ObjectPoolService/FetchIntoObjectPool",
			request: &gitalypb.FetchIntoObjectPoolRequest{
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  "something",
						RelativePath: "something",
					},
				},
			},
		},
	} {
		b.Run(bc.desc, func(b *testing.B) {
			mi, err := GitalyProtoPreregistered.LookupMethod(bc.method)
			require.NoError(b, err)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := mi.TargetRepo(bc.request)
				require.Equal(b, bc.expectedErr, err)
			}
		})
	}
}
