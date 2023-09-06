package repository

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func createRepositoryRequest() *gitalypb.CreateRepositoryRequest {
	return &gitalypb.CreateRepositoryRequest{
		Repository: &gitalypb.Repository{
			StorageName:                   "default",
			RelativePath:                  "test/perf/services/repository/createRepository/" + fmt.Sprint(time.Now().UnixNano()),
			GitObjectDirectory:            "",
			GitAlternateObjectDirectories: nil,
			GlRepository:                  "",
			GlProjectPath:                 "",
		},
		DefaultBranch: []byte("createRepository"),
		ObjectFormat:  gitalypb.ObjectFormat_OBJECT_FORMAT_SHA1,
	}
}

func Benchmark_CreateRepository(b *testing.B) {
	cfg := testcfg.Build(b)
	cfg.SocketPath = testserver.RunGitalyServer(b, cfg, setup.RegisterAll)
	conn, _ := grpc.Dial(cfg.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer testhelper.MustClose(b, conn)
	client := gitalypb.NewRepositoryServiceClient(conn)

	for i := 0; i < b.N; i++ {
		repo, err := client.CreateRepository(context.Background(), createRepositoryRequest())
		if err != nil {
			panic(err)
		}
		assert.Empty(b, repo.String())
	}
}
