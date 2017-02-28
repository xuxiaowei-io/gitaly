package ref

import (
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
)

const (
	scratchDir   = "testdata/scratch"
	testRepoRoot = "testdata/data"
	testRepo     = "group/test.git"
)

var serverSocketPath = path.Join(scratchDir, "gitaly.sock")

func containsRef(refs [][]byte, ref string) bool {
	for _, b := range refs {
		if string(b) == ref {
			return true
		}
	}
	return false
}

func TestMain(m *testing.M) {
	source := "https://gitlab.com/gitlab-org/gitlab-test.git"
	clonePath := path.Join(testRepoRoot, testRepo)
	if _, err := os.Stat(clonePath); err != nil {
		testCmd := exec.Command("git", "clone", "--bare", source, clonePath)
		testCmd.Stdout = os.Stdout
		testCmd.Stderr = os.Stderr

		if err := testCmd.Run(); err != nil {
			log.Printf("Test setup: failed to run %v", testCmd)
			os.Exit(-1)
		}
	}

	if err := os.MkdirAll(scratchDir, 0755); err != nil {
		log.Fatal(err)
	}

	os.Exit(func() int {
		return m.Run()
	}())
}

func TestSuccessfulFindAllBranchNames(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: path.Join(testRepoRoot, testRepo)}
	rpcRequest := &pb.FindAllBranchNamesRequest{Repository: repo}

	c, err := client.FindAllBranchNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var names [][]byte
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, r.GetNames()...)
	}

	for _, branch := range []string{"master", "100%branch", "improve/awesome", "'test'"} {
		if !containsRef(names, "refs/heads/"+branch) {
			t.Fatalf("Expected to find branch %q in all branch names", branch)
		}
	}
}

func TestEmptyFindAllBranchNamesRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	rpcRequest := &pb.FindAllBranchNamesRequest{}

	c, err := client.FindAllBranchNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if grpc.Code(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}

func TestSuccessfulFindAllTagNames(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: path.Join(testRepoRoot, testRepo)}
	rpcRequest := &pb.FindAllTagNamesRequest{Repository: repo}

	c, err := client.FindAllTagNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var names [][]byte
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, r.GetNames()...)
	}

	for _, tag := range []string{"v1.0.0", "v1.1.0"} {
		if !containsRef(names, "refs/tags/"+tag) {
			t.Fatal("Expected to find tag", tag, "in all tag names")
		}
	}
}

func TestEmptyFindAllTagNamesRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	rpcRequest := &pb.FindAllTagNamesRequest{}

	c, err := client.FindAllTagNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if grpc.Code(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}

func TestHeadReference(t *testing.T) {
	headRef, err := headReference(path.Join(testRepoRoot, testRepo))
	if err != nil {
		t.Fatal(err)
	}
	if string(headRef) != "refs/heads/master" {
		t.Fatal("Expected HEAD reference to be 'ref/heads/master', got '", string(headRef), "'")
	}
}

func TestDefaultBranchName(t *testing.T) {
	// We are going to override these functions during this test. Restore them after we're done
	defer func() {
		findBranchNames = _findBranchNames
		headReference = _headReference
	}()

	testCases := []struct {
		desc            string
		findBranchNames func(string) ([][]byte, error)
		headReference   func(string) ([]byte, error)
		expected        []byte
	}{
		{
			desc:     "Get first branch when only one branch exists",
			expected: []byte("refs/heads/foo"),
			findBranchNames: func(string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo")}, nil
			},
			headReference: func(string) ([]byte, error) { return nil, nil },
		},
		{
			desc:            "Get empy ref if no branches exists",
			expected:        nil,
			findBranchNames: func(string) ([][]byte, error) { return [][]byte{}, nil },
			headReference:   func(string) ([]byte, error) { return nil, nil },
		},
		{
			desc:     "Get the name of the head reference when more than one branch exists",
			expected: []byte("refs/heads/bar"),
			findBranchNames: func(string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/bar")}, nil
			},
			headReference: func(string) ([]byte, error) { return []byte("refs/heads/bar"), nil },
		},
		{
			desc:     "Get `ref/heads/master` when several branches exist",
			expected: []byte("refs/heads/master"),
			findBranchNames: func(string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/master"), []byte("refs/heads/bar")}, nil
			},
			headReference: func(string) ([]byte, error) { return nil, nil },
		},
		{
			desc:     "Get the name of the first branch when several branches exists and no other conditions are met",
			expected: []byte("refs/heads/foo"),
			findBranchNames: func(string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/bar"), []byte("refs/heads/baz")}, nil
			},
			headReference: func(string) ([]byte, error) { return nil, nil },
		},
	}

	for _, testCase := range testCases {
		findBranchNames = testCase.findBranchNames
		headReference = testCase.headReference

		defaultBranch, err := defaultBranchName("")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(defaultBranch, testCase.expected) {
			t.Fatalf("%s: expected %s, got %s instead", testCase.desc, testCase.expected, defaultBranch)
		}
	}
}

func TestSuccessfulFindDefaultBranchName(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: path.Join(testRepoRoot, testRepo)}
	rpcRequest := &pb.FindDefaultBranchNameRequest{Repository: repo}

	r, err := client.FindDefaultBranchName(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	if name := r.GetName(); string(name) != "refs/heads/master" {
		t.Fatal("Expected HEAD reference to be 'ref/heads/master', got '", string(name), "'")
	}
}

func TestEmptyFindDefaultBranchNameRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	rpcRequest := &pb.FindDefaultBranchNameRequest{}

	_, err := client.FindDefaultBranchName(context.Background(), rpcRequest)

	if grpc.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func runRefServer(t *testing.T) *grpc.Server {
	grpcServer := grpc.NewServer()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	// Use 100 bytes as the maximum message size to test that fragmenting the ref list works correctly
	pb.RegisterRefServer(grpcServer, &server{MaxMsgSize: 100})
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer
}

func newRefClient(t *testing.T) pb.RefClient {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewRefClient(conn)
}
