package repositoryService_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gitlab.com/gitlab-org/gitaly/v15/internal/qa/helpers"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var _ = Describe("CreateRepository", func() {
	var (
		client gitalypb.RepositoryServiceClient
		conn   *grpc.ClientConn
		err    error
	)

	BeforeEach(func() {
		//socketPath := filepath.Join(cfg.Storages[0].Path, "/Users/john/dev/gdk", "gitaly.socket")
		conn, err = grpc.Dial("gdk.test:9999", grpc.WithInsecure())
		Expect(err).NotTo(HaveOccurred())

		client = gitalypb.NewRepositoryServiceClient(conn)
	})

	AfterEach(func() {
		conn.Close()
	})

	It("should create a repository successfully", func() {
		repo := helpers.NewRepository()

		resp, err := client.CreateRepository(context.Background(), &gitalypb.CreateRepositoryRequest{
			Repository: repo,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		// TODO: Assert that repository was created successfully
	})
})
