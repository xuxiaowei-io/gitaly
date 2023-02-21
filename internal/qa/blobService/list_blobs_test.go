package blobService

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var _ = Describe("BlobService", func() {
	var (
		client gitalypb.BlobServiceClient
		conn   *grpc.ClientConn
		err    error
	)

	BeforeEach(func() {
		//socketPath := filepath.Join(cfg.Storages[0].Path, "/Users/john/dev/gdk", "gitaly.socket")
		conn, err = grpc.Dial("gdk.test:9999", grpc.WithInsecure())
		Expect(err).NotTo(HaveOccurred())

		client = gitalypb.NewBlobServiceClient(conn)
	})

	AfterEach(func() {
		conn.Close()
	})

	It("should list reachable blobs for a given revision", func() {
		repo := &gitalypb.Repository{
			StorageName:  "default",
			RelativePath: "@hashed/2f/ca/2fca346db656187102ce806ac732e06a62df0dbb2829e511a770556d398e1a6e.git",
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		request := &gitalypb.ListBlobsRequest{
			Repository: repo,
			Revisions:  []string{"main"},
		}
		stream, err := client.ListBlobs(ctx, request)

		var response *gitalypb.ListBlobsResponse
		var blobs []*gitalypb.ListBlobsResponse_Blob
		var count int
		for {
			response, err = stream.Recv()
			if err != nil {
				break
			}
			count += len(response.Blobs)
			blobs = append(blobs, response.Blobs...)
		}

		for i, blob := range blobs {
			fmt.Println("BLOB FOUND", i, blob)
		}

		Expect(count).To(Equal(2))
	})
})
