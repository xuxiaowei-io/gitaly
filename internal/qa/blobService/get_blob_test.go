package blobService

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("BlobService", func() {
	//var (
	//	client gitalypb.BlobServiceClient
	//	conn   *grpc.ClientConn
	//	err    error
	//)
	//
	//BeforeEach(func() {
	//	//socketPath := filepath.Join(cfg.Storages[0].Path, "/Users/john/dev/gdk", "gitaly.socket")
	//	conn, err = grpc.Dial("gdk.test:9999", grpc.WithInsecure())
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	client = gitalypb.NewBlobServiceClient(conn)
	//})
	//
	//AfterEach(func() {
	//	conn.Close()
	//})
	//
	//It("should get a blob from a repository", func() {
	//	repo := &gitalypb.Repository{
	//		StorageName:  "default",
	//		RelativePath: "@hashed/2f/ca/2fca346db656187102ce806ac732e06a62df0dbb2829e511a770556d398e1a6e.git",
	//	}
	//	ctx, cancel := context.WithCancel(context.Background())
	//	defer cancel()
	//
	//	request := &gitalypb.GetBlobRequest{
	//		Repository: repo,
	//		Oid:        "ee1dec81eda581d28e4dfaeba9f6d8f17b909fff",
	//		Limit:      -1,
	//	}
	//	stream, err := client.GetBlob(ctx, request)
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	// read blob data
	//	var data []byte
	//	for {
	//		response, err := stream.Recv()
	//		fmt.Println("response: ", response)
	//		if err != nil {
	//			Expect(err).To(Equal(io.EOF))
	//			break
	//		}
	//		data = append(data, response.Data...)
	//	}
	//
	//	Expect(string(data)).To(Equal("ginkgo-testing-2\n"))
	//})
})
