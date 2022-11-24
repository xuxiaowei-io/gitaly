//go:build !gitaly_test_sha256

package repository

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestWithRubySidecar(t *testing.T) {
	t.Parallel()
	cfg := testcfg.Build(t)

	rubySrv := rubyserver.New(cfg, gittest.NewCommandFactory(t, cfg))
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	client, serverSocketPath := runRepositoryService(t, cfg, rubySrv)
	cfg.SocketPath = serverSocketPath

	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalyGit2Go(t, cfg)

	fs := []func(t *testing.T, cfg config.Cfg, client gitalypb.RepositoryServiceClient, rubySrv *rubyserver.Server){
		testSuccessfulFindLicenseRequest,
		testFindLicenseRequestEmptyRepo,
	}
	for _, f := range fs {
		t.Run(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), func(t *testing.T) {
			f(t, cfg, client, rubySrv)
		})
	}
}
