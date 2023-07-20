// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.23.1
// source: ref.proto

package gitalypb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// RefServiceClient is the client API for RefService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RefServiceClient interface {
	// FindDefaultBranchName looks up the default branch reference name. Unless
	// otherwise specified the following heuristic is used:
	//
	// 1. If there are no branches, return an empty string.
	// 2. If there is only one branch, return the only branch.
	// 3. If a branch exists that matches HEAD, return the HEAD reference name.
	// 4. If a branch exists named refs/heads/main, return refs/heads/main.
	// 5. If a branch exists named refs/heads/master, return refs/heads/master.
	// 6. Return the first branch (as per default ordering by git).
	FindDefaultBranchName(ctx context.Context, in *FindDefaultBranchNameRequest, opts ...grpc.CallOption) (*FindDefaultBranchNameResponse, error)
	// Return a stream so we can divide the response in chunks of branches
	FindLocalBranches(ctx context.Context, in *FindLocalBranchesRequest, opts ...grpc.CallOption) (RefService_FindLocalBranchesClient, error)
	// This comment is left unintentionally blank.
	FindAllBranches(ctx context.Context, in *FindAllBranchesRequest, opts ...grpc.CallOption) (RefService_FindAllBranchesClient, error)
	// Returns a stream of tags repository has.
	FindAllTags(ctx context.Context, in *FindAllTagsRequest, opts ...grpc.CallOption) (RefService_FindAllTagsClient, error)
	// FindTag looks up a tag by its name and returns it to the caller if it exists. This RPC supports
	// both lightweight and annotated tags. Note: this RPC returns an `Internal` error if the tag was
	// not found.
	FindTag(ctx context.Context, in *FindTagRequest, opts ...grpc.CallOption) (*FindTagResponse, error)
	// This comment is left unintentionally blank.
	FindAllRemoteBranches(ctx context.Context, in *FindAllRemoteBranchesRequest, opts ...grpc.CallOption) (RefService_FindAllRemoteBranchesClient, error)
	// This comment is left unintentionally blank.
	RefExists(ctx context.Context, in *RefExistsRequest, opts ...grpc.CallOption) (*RefExistsResponse, error)
	// FindBranch finds a branch by its unqualified name (like "master") and
	// returns the commit it currently points to.
	FindBranch(ctx context.Context, in *FindBranchRequest, opts ...grpc.CallOption) (*FindBranchResponse, error)
	// UpdateReferences atomically updates a set of references to a new state. This RPC allows creating
	// new references, deleting old references and updating existing references in a raceless way.
	//
	// Updating symbolic references with this RPC is not allowed.
	UpdateReferences(ctx context.Context, opts ...grpc.CallOption) (RefService_UpdateReferencesClient, error)
	// This comment is left unintentionally blank.
	DeleteRefs(ctx context.Context, in *DeleteRefsRequest, opts ...grpc.CallOption) (*DeleteRefsResponse, error)
	// This comment is left unintentionally blank.
	ListBranchNamesContainingCommit(ctx context.Context, in *ListBranchNamesContainingCommitRequest, opts ...grpc.CallOption) (RefService_ListBranchNamesContainingCommitClient, error)
	// This comment is left unintentionally blank.
	ListTagNamesContainingCommit(ctx context.Context, in *ListTagNamesContainingCommitRequest, opts ...grpc.CallOption) (RefService_ListTagNamesContainingCommitClient, error)
	// GetTagSignatures returns signatures for annotated tags resolved from a set of revisions. Revisions
	// which don't resolve to an annotated tag are silently discarded. Revisions which cannot be resolved
	// result in an error. Tags which are annotated but not signed will return a TagSignature response
	// which has no signature, but its unsigned contents will still be returned.
	GetTagSignatures(ctx context.Context, in *GetTagSignaturesRequest, opts ...grpc.CallOption) (RefService_GetTagSignaturesClient, error)
	// This comment is left unintentionally blank.
	GetTagMessages(ctx context.Context, in *GetTagMessagesRequest, opts ...grpc.CallOption) (RefService_GetTagMessagesClient, error)
	// ListRefs returns a stream of all references in the repository. By default, pseudo-revisions like HEAD
	// will not be returned by this RPC. Any symbolic references will be resolved to the object ID it is
	// pointing at.
	ListRefs(ctx context.Context, in *ListRefsRequest, opts ...grpc.CallOption) (RefService_ListRefsClient, error)
	// FindRefsByOID returns an array of fully qualified reference names that point to an object ID.
	// It returns nothing if the object ID doesn't exist, or doesn't point to
	// any branches or tags. Prefixes can be also be used as the object ID.
	FindRefsByOID(ctx context.Context, in *FindRefsByOIDRequest, opts ...grpc.CallOption) (*FindRefsByOIDResponse, error)
}

type refServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRefServiceClient(cc grpc.ClientConnInterface) RefServiceClient {
	return &refServiceClient{cc}
}

func (c *refServiceClient) FindDefaultBranchName(ctx context.Context, in *FindDefaultBranchNameRequest, opts ...grpc.CallOption) (*FindDefaultBranchNameResponse, error) {
	out := new(FindDefaultBranchNameResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefService/FindDefaultBranchName", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *refServiceClient) FindLocalBranches(ctx context.Context, in *FindLocalBranchesRequest, opts ...grpc.CallOption) (RefService_FindLocalBranchesClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[0], "/gitaly.RefService/FindLocalBranches", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceFindLocalBranchesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_FindLocalBranchesClient interface {
	Recv() (*FindLocalBranchesResponse, error)
	grpc.ClientStream
}

type refServiceFindLocalBranchesClient struct {
	grpc.ClientStream
}

func (x *refServiceFindLocalBranchesClient) Recv() (*FindLocalBranchesResponse, error) {
	m := new(FindLocalBranchesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) FindAllBranches(ctx context.Context, in *FindAllBranchesRequest, opts ...grpc.CallOption) (RefService_FindAllBranchesClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[1], "/gitaly.RefService/FindAllBranches", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceFindAllBranchesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_FindAllBranchesClient interface {
	Recv() (*FindAllBranchesResponse, error)
	grpc.ClientStream
}

type refServiceFindAllBranchesClient struct {
	grpc.ClientStream
}

func (x *refServiceFindAllBranchesClient) Recv() (*FindAllBranchesResponse, error) {
	m := new(FindAllBranchesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) FindAllTags(ctx context.Context, in *FindAllTagsRequest, opts ...grpc.CallOption) (RefService_FindAllTagsClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[2], "/gitaly.RefService/FindAllTags", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceFindAllTagsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_FindAllTagsClient interface {
	Recv() (*FindAllTagsResponse, error)
	grpc.ClientStream
}

type refServiceFindAllTagsClient struct {
	grpc.ClientStream
}

func (x *refServiceFindAllTagsClient) Recv() (*FindAllTagsResponse, error) {
	m := new(FindAllTagsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) FindTag(ctx context.Context, in *FindTagRequest, opts ...grpc.CallOption) (*FindTagResponse, error) {
	out := new(FindTagResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefService/FindTag", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *refServiceClient) FindAllRemoteBranches(ctx context.Context, in *FindAllRemoteBranchesRequest, opts ...grpc.CallOption) (RefService_FindAllRemoteBranchesClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[3], "/gitaly.RefService/FindAllRemoteBranches", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceFindAllRemoteBranchesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_FindAllRemoteBranchesClient interface {
	Recv() (*FindAllRemoteBranchesResponse, error)
	grpc.ClientStream
}

type refServiceFindAllRemoteBranchesClient struct {
	grpc.ClientStream
}

func (x *refServiceFindAllRemoteBranchesClient) Recv() (*FindAllRemoteBranchesResponse, error) {
	m := new(FindAllRemoteBranchesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) RefExists(ctx context.Context, in *RefExistsRequest, opts ...grpc.CallOption) (*RefExistsResponse, error) {
	out := new(RefExistsResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefService/RefExists", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *refServiceClient) FindBranch(ctx context.Context, in *FindBranchRequest, opts ...grpc.CallOption) (*FindBranchResponse, error) {
	out := new(FindBranchResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefService/FindBranch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *refServiceClient) UpdateReferences(ctx context.Context, opts ...grpc.CallOption) (RefService_UpdateReferencesClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[4], "/gitaly.RefService/UpdateReferences", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceUpdateReferencesClient{stream}
	return x, nil
}

type RefService_UpdateReferencesClient interface {
	Send(*UpdateReferencesRequest) error
	CloseAndRecv() (*UpdateReferencesResponse, error)
	grpc.ClientStream
}

type refServiceUpdateReferencesClient struct {
	grpc.ClientStream
}

func (x *refServiceUpdateReferencesClient) Send(m *UpdateReferencesRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *refServiceUpdateReferencesClient) CloseAndRecv() (*UpdateReferencesResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(UpdateReferencesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) DeleteRefs(ctx context.Context, in *DeleteRefsRequest, opts ...grpc.CallOption) (*DeleteRefsResponse, error) {
	out := new(DeleteRefsResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefService/DeleteRefs", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *refServiceClient) ListBranchNamesContainingCommit(ctx context.Context, in *ListBranchNamesContainingCommitRequest, opts ...grpc.CallOption) (RefService_ListBranchNamesContainingCommitClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[5], "/gitaly.RefService/ListBranchNamesContainingCommit", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceListBranchNamesContainingCommitClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_ListBranchNamesContainingCommitClient interface {
	Recv() (*ListBranchNamesContainingCommitResponse, error)
	grpc.ClientStream
}

type refServiceListBranchNamesContainingCommitClient struct {
	grpc.ClientStream
}

func (x *refServiceListBranchNamesContainingCommitClient) Recv() (*ListBranchNamesContainingCommitResponse, error) {
	m := new(ListBranchNamesContainingCommitResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) ListTagNamesContainingCommit(ctx context.Context, in *ListTagNamesContainingCommitRequest, opts ...grpc.CallOption) (RefService_ListTagNamesContainingCommitClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[6], "/gitaly.RefService/ListTagNamesContainingCommit", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceListTagNamesContainingCommitClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_ListTagNamesContainingCommitClient interface {
	Recv() (*ListTagNamesContainingCommitResponse, error)
	grpc.ClientStream
}

type refServiceListTagNamesContainingCommitClient struct {
	grpc.ClientStream
}

func (x *refServiceListTagNamesContainingCommitClient) Recv() (*ListTagNamesContainingCommitResponse, error) {
	m := new(ListTagNamesContainingCommitResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) GetTagSignatures(ctx context.Context, in *GetTagSignaturesRequest, opts ...grpc.CallOption) (RefService_GetTagSignaturesClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[7], "/gitaly.RefService/GetTagSignatures", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceGetTagSignaturesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_GetTagSignaturesClient interface {
	Recv() (*GetTagSignaturesResponse, error)
	grpc.ClientStream
}

type refServiceGetTagSignaturesClient struct {
	grpc.ClientStream
}

func (x *refServiceGetTagSignaturesClient) Recv() (*GetTagSignaturesResponse, error) {
	m := new(GetTagSignaturesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) GetTagMessages(ctx context.Context, in *GetTagMessagesRequest, opts ...grpc.CallOption) (RefService_GetTagMessagesClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[8], "/gitaly.RefService/GetTagMessages", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceGetTagMessagesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_GetTagMessagesClient interface {
	Recv() (*GetTagMessagesResponse, error)
	grpc.ClientStream
}

type refServiceGetTagMessagesClient struct {
	grpc.ClientStream
}

func (x *refServiceGetTagMessagesClient) Recv() (*GetTagMessagesResponse, error) {
	m := new(GetTagMessagesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) ListRefs(ctx context.Context, in *ListRefsRequest, opts ...grpc.CallOption) (RefService_ListRefsClient, error) {
	stream, err := c.cc.NewStream(ctx, &RefService_ServiceDesc.Streams[9], "/gitaly.RefService/ListRefs", opts...)
	if err != nil {
		return nil, err
	}
	x := &refServiceListRefsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RefService_ListRefsClient interface {
	Recv() (*ListRefsResponse, error)
	grpc.ClientStream
}

type refServiceListRefsClient struct {
	grpc.ClientStream
}

func (x *refServiceListRefsClient) Recv() (*ListRefsResponse, error) {
	m := new(ListRefsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *refServiceClient) FindRefsByOID(ctx context.Context, in *FindRefsByOIDRequest, opts ...grpc.CallOption) (*FindRefsByOIDResponse, error) {
	out := new(FindRefsByOIDResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefService/FindRefsByOID", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RefServiceServer is the server API for RefService service.
// All implementations must embed UnimplementedRefServiceServer
// for forward compatibility
type RefServiceServer interface {
	// FindDefaultBranchName looks up the default branch reference name. Unless
	// otherwise specified the following heuristic is used:
	//
	// 1. If there are no branches, return an empty string.
	// 2. If there is only one branch, return the only branch.
	// 3. If a branch exists that matches HEAD, return the HEAD reference name.
	// 4. If a branch exists named refs/heads/main, return refs/heads/main.
	// 5. If a branch exists named refs/heads/master, return refs/heads/master.
	// 6. Return the first branch (as per default ordering by git).
	FindDefaultBranchName(context.Context, *FindDefaultBranchNameRequest) (*FindDefaultBranchNameResponse, error)
	// Return a stream so we can divide the response in chunks of branches
	FindLocalBranches(*FindLocalBranchesRequest, RefService_FindLocalBranchesServer) error
	// This comment is left unintentionally blank.
	FindAllBranches(*FindAllBranchesRequest, RefService_FindAllBranchesServer) error
	// Returns a stream of tags repository has.
	FindAllTags(*FindAllTagsRequest, RefService_FindAllTagsServer) error
	// FindTag looks up a tag by its name and returns it to the caller if it exists. This RPC supports
	// both lightweight and annotated tags. Note: this RPC returns an `Internal` error if the tag was
	// not found.
	FindTag(context.Context, *FindTagRequest) (*FindTagResponse, error)
	// This comment is left unintentionally blank.
	FindAllRemoteBranches(*FindAllRemoteBranchesRequest, RefService_FindAllRemoteBranchesServer) error
	// This comment is left unintentionally blank.
	RefExists(context.Context, *RefExistsRequest) (*RefExistsResponse, error)
	// FindBranch finds a branch by its unqualified name (like "master") and
	// returns the commit it currently points to.
	FindBranch(context.Context, *FindBranchRequest) (*FindBranchResponse, error)
	// UpdateReferences atomically updates a set of references to a new state. This RPC allows creating
	// new references, deleting old references and updating existing references in a raceless way.
	//
	// Updating symbolic references with this RPC is not allowed.
	UpdateReferences(RefService_UpdateReferencesServer) error
	// This comment is left unintentionally blank.
	DeleteRefs(context.Context, *DeleteRefsRequest) (*DeleteRefsResponse, error)
	// This comment is left unintentionally blank.
	ListBranchNamesContainingCommit(*ListBranchNamesContainingCommitRequest, RefService_ListBranchNamesContainingCommitServer) error
	// This comment is left unintentionally blank.
	ListTagNamesContainingCommit(*ListTagNamesContainingCommitRequest, RefService_ListTagNamesContainingCommitServer) error
	// GetTagSignatures returns signatures for annotated tags resolved from a set of revisions. Revisions
	// which don't resolve to an annotated tag are silently discarded. Revisions which cannot be resolved
	// result in an error. Tags which are annotated but not signed will return a TagSignature response
	// which has no signature, but its unsigned contents will still be returned.
	GetTagSignatures(*GetTagSignaturesRequest, RefService_GetTagSignaturesServer) error
	// This comment is left unintentionally blank.
	GetTagMessages(*GetTagMessagesRequest, RefService_GetTagMessagesServer) error
	// ListRefs returns a stream of all references in the repository. By default, pseudo-revisions like HEAD
	// will not be returned by this RPC. Any symbolic references will be resolved to the object ID it is
	// pointing at.
	ListRefs(*ListRefsRequest, RefService_ListRefsServer) error
	// FindRefsByOID returns an array of fully qualified reference names that point to an object ID.
	// It returns nothing if the object ID doesn't exist, or doesn't point to
	// any branches or tags. Prefixes can be also be used as the object ID.
	FindRefsByOID(context.Context, *FindRefsByOIDRequest) (*FindRefsByOIDResponse, error)
	mustEmbedUnimplementedRefServiceServer()
}

// UnimplementedRefServiceServer must be embedded to have forward compatible implementations.
type UnimplementedRefServiceServer struct {
}

func (UnimplementedRefServiceServer) FindDefaultBranchName(context.Context, *FindDefaultBranchNameRequest) (*FindDefaultBranchNameResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FindDefaultBranchName not implemented")
}
func (UnimplementedRefServiceServer) FindLocalBranches(*FindLocalBranchesRequest, RefService_FindLocalBranchesServer) error {
	return status.Errorf(codes.Unimplemented, "method FindLocalBranches not implemented")
}
func (UnimplementedRefServiceServer) FindAllBranches(*FindAllBranchesRequest, RefService_FindAllBranchesServer) error {
	return status.Errorf(codes.Unimplemented, "method FindAllBranches not implemented")
}
func (UnimplementedRefServiceServer) FindAllTags(*FindAllTagsRequest, RefService_FindAllTagsServer) error {
	return status.Errorf(codes.Unimplemented, "method FindAllTags not implemented")
}
func (UnimplementedRefServiceServer) FindTag(context.Context, *FindTagRequest) (*FindTagResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FindTag not implemented")
}
func (UnimplementedRefServiceServer) FindAllRemoteBranches(*FindAllRemoteBranchesRequest, RefService_FindAllRemoteBranchesServer) error {
	return status.Errorf(codes.Unimplemented, "method FindAllRemoteBranches not implemented")
}
func (UnimplementedRefServiceServer) RefExists(context.Context, *RefExistsRequest) (*RefExistsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RefExists not implemented")
}
func (UnimplementedRefServiceServer) FindBranch(context.Context, *FindBranchRequest) (*FindBranchResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FindBranch not implemented")
}
func (UnimplementedRefServiceServer) UpdateReferences(RefService_UpdateReferencesServer) error {
	return status.Errorf(codes.Unimplemented, "method UpdateReferences not implemented")
}
func (UnimplementedRefServiceServer) DeleteRefs(context.Context, *DeleteRefsRequest) (*DeleteRefsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteRefs not implemented")
}
func (UnimplementedRefServiceServer) ListBranchNamesContainingCommit(*ListBranchNamesContainingCommitRequest, RefService_ListBranchNamesContainingCommitServer) error {
	return status.Errorf(codes.Unimplemented, "method ListBranchNamesContainingCommit not implemented")
}
func (UnimplementedRefServiceServer) ListTagNamesContainingCommit(*ListTagNamesContainingCommitRequest, RefService_ListTagNamesContainingCommitServer) error {
	return status.Errorf(codes.Unimplemented, "method ListTagNamesContainingCommit not implemented")
}
func (UnimplementedRefServiceServer) GetTagSignatures(*GetTagSignaturesRequest, RefService_GetTagSignaturesServer) error {
	return status.Errorf(codes.Unimplemented, "method GetTagSignatures not implemented")
}
func (UnimplementedRefServiceServer) GetTagMessages(*GetTagMessagesRequest, RefService_GetTagMessagesServer) error {
	return status.Errorf(codes.Unimplemented, "method GetTagMessages not implemented")
}
func (UnimplementedRefServiceServer) ListRefs(*ListRefsRequest, RefService_ListRefsServer) error {
	return status.Errorf(codes.Unimplemented, "method ListRefs not implemented")
}
func (UnimplementedRefServiceServer) FindRefsByOID(context.Context, *FindRefsByOIDRequest) (*FindRefsByOIDResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FindRefsByOID not implemented")
}
func (UnimplementedRefServiceServer) mustEmbedUnimplementedRefServiceServer() {}

// UnsafeRefServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RefServiceServer will
// result in compilation errors.
type UnsafeRefServiceServer interface {
	mustEmbedUnimplementedRefServiceServer()
}

func RegisterRefServiceServer(s grpc.ServiceRegistrar, srv RefServiceServer) {
	s.RegisterService(&RefService_ServiceDesc, srv)
}

func _RefService_FindDefaultBranchName_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FindDefaultBranchNameRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefServiceServer).FindDefaultBranchName(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefService/FindDefaultBranchName",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefServiceServer).FindDefaultBranchName(ctx, req.(*FindDefaultBranchNameRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RefService_FindLocalBranches_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FindLocalBranchesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).FindLocalBranches(m, &refServiceFindLocalBranchesServer{stream})
}

type RefService_FindLocalBranchesServer interface {
	Send(*FindLocalBranchesResponse) error
	grpc.ServerStream
}

type refServiceFindLocalBranchesServer struct {
	grpc.ServerStream
}

func (x *refServiceFindLocalBranchesServer) Send(m *FindLocalBranchesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_FindAllBranches_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FindAllBranchesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).FindAllBranches(m, &refServiceFindAllBranchesServer{stream})
}

type RefService_FindAllBranchesServer interface {
	Send(*FindAllBranchesResponse) error
	grpc.ServerStream
}

type refServiceFindAllBranchesServer struct {
	grpc.ServerStream
}

func (x *refServiceFindAllBranchesServer) Send(m *FindAllBranchesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_FindAllTags_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FindAllTagsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).FindAllTags(m, &refServiceFindAllTagsServer{stream})
}

type RefService_FindAllTagsServer interface {
	Send(*FindAllTagsResponse) error
	grpc.ServerStream
}

type refServiceFindAllTagsServer struct {
	grpc.ServerStream
}

func (x *refServiceFindAllTagsServer) Send(m *FindAllTagsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_FindTag_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FindTagRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefServiceServer).FindTag(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefService/FindTag",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefServiceServer).FindTag(ctx, req.(*FindTagRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RefService_FindAllRemoteBranches_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FindAllRemoteBranchesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).FindAllRemoteBranches(m, &refServiceFindAllRemoteBranchesServer{stream})
}

type RefService_FindAllRemoteBranchesServer interface {
	Send(*FindAllRemoteBranchesResponse) error
	grpc.ServerStream
}

type refServiceFindAllRemoteBranchesServer struct {
	grpc.ServerStream
}

func (x *refServiceFindAllRemoteBranchesServer) Send(m *FindAllRemoteBranchesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_RefExists_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RefExistsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefServiceServer).RefExists(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefService/RefExists",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefServiceServer).RefExists(ctx, req.(*RefExistsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RefService_FindBranch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FindBranchRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefServiceServer).FindBranch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefService/FindBranch",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefServiceServer).FindBranch(ctx, req.(*FindBranchRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RefService_UpdateReferences_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RefServiceServer).UpdateReferences(&refServiceUpdateReferencesServer{stream})
}

type RefService_UpdateReferencesServer interface {
	SendAndClose(*UpdateReferencesResponse) error
	Recv() (*UpdateReferencesRequest, error)
	grpc.ServerStream
}

type refServiceUpdateReferencesServer struct {
	grpc.ServerStream
}

func (x *refServiceUpdateReferencesServer) SendAndClose(m *UpdateReferencesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *refServiceUpdateReferencesServer) Recv() (*UpdateReferencesRequest, error) {
	m := new(UpdateReferencesRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _RefService_DeleteRefs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRefsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefServiceServer).DeleteRefs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefService/DeleteRefs",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefServiceServer).DeleteRefs(ctx, req.(*DeleteRefsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RefService_ListBranchNamesContainingCommit_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListBranchNamesContainingCommitRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).ListBranchNamesContainingCommit(m, &refServiceListBranchNamesContainingCommitServer{stream})
}

type RefService_ListBranchNamesContainingCommitServer interface {
	Send(*ListBranchNamesContainingCommitResponse) error
	grpc.ServerStream
}

type refServiceListBranchNamesContainingCommitServer struct {
	grpc.ServerStream
}

func (x *refServiceListBranchNamesContainingCommitServer) Send(m *ListBranchNamesContainingCommitResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_ListTagNamesContainingCommit_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListTagNamesContainingCommitRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).ListTagNamesContainingCommit(m, &refServiceListTagNamesContainingCommitServer{stream})
}

type RefService_ListTagNamesContainingCommitServer interface {
	Send(*ListTagNamesContainingCommitResponse) error
	grpc.ServerStream
}

type refServiceListTagNamesContainingCommitServer struct {
	grpc.ServerStream
}

func (x *refServiceListTagNamesContainingCommitServer) Send(m *ListTagNamesContainingCommitResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_GetTagSignatures_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(GetTagSignaturesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).GetTagSignatures(m, &refServiceGetTagSignaturesServer{stream})
}

type RefService_GetTagSignaturesServer interface {
	Send(*GetTagSignaturesResponse) error
	grpc.ServerStream
}

type refServiceGetTagSignaturesServer struct {
	grpc.ServerStream
}

func (x *refServiceGetTagSignaturesServer) Send(m *GetTagSignaturesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_GetTagMessages_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(GetTagMessagesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).GetTagMessages(m, &refServiceGetTagMessagesServer{stream})
}

type RefService_GetTagMessagesServer interface {
	Send(*GetTagMessagesResponse) error
	grpc.ServerStream
}

type refServiceGetTagMessagesServer struct {
	grpc.ServerStream
}

func (x *refServiceGetTagMessagesServer) Send(m *GetTagMessagesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_ListRefs_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListRefsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RefServiceServer).ListRefs(m, &refServiceListRefsServer{stream})
}

type RefService_ListRefsServer interface {
	Send(*ListRefsResponse) error
	grpc.ServerStream
}

type refServiceListRefsServer struct {
	grpc.ServerStream
}

func (x *refServiceListRefsServer) Send(m *ListRefsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RefService_FindRefsByOID_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FindRefsByOIDRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefServiceServer).FindRefsByOID(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefService/FindRefsByOID",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefServiceServer).FindRefsByOID(ctx, req.(*FindRefsByOIDRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RefService_ServiceDesc is the grpc.ServiceDesc for RefService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RefService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.RefService",
	HandlerType: (*RefServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FindDefaultBranchName",
			Handler:    _RefService_FindDefaultBranchName_Handler,
		},
		{
			MethodName: "FindTag",
			Handler:    _RefService_FindTag_Handler,
		},
		{
			MethodName: "RefExists",
			Handler:    _RefService_RefExists_Handler,
		},
		{
			MethodName: "FindBranch",
			Handler:    _RefService_FindBranch_Handler,
		},
		{
			MethodName: "DeleteRefs",
			Handler:    _RefService_DeleteRefs_Handler,
		},
		{
			MethodName: "FindRefsByOID",
			Handler:    _RefService_FindRefsByOID_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "FindLocalBranches",
			Handler:       _RefService_FindLocalBranches_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "FindAllBranches",
			Handler:       _RefService_FindAllBranches_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "FindAllTags",
			Handler:       _RefService_FindAllTags_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "FindAllRemoteBranches",
			Handler:       _RefService_FindAllRemoteBranches_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "UpdateReferences",
			Handler:       _RefService_UpdateReferences_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "ListBranchNamesContainingCommit",
			Handler:       _RefService_ListBranchNamesContainingCommit_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ListTagNamesContainingCommit",
			Handler:       _RefService_ListTagNamesContainingCommit_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "GetTagSignatures",
			Handler:       _RefService_GetTagSignatures_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "GetTagMessages",
			Handler:       _RefService_GetTagMessages_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ListRefs",
			Handler:       _RefService_ListRefs_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "ref.proto",
}
