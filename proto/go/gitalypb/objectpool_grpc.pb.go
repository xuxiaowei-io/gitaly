// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

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

// ObjectPoolServiceClient is the client API for ObjectPoolService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ObjectPoolServiceClient interface {
	CreateObjectPool(ctx context.Context, in *CreateObjectPoolRequest, opts ...grpc.CallOption) (*CreateObjectPoolResponse, error)
	DeleteObjectPool(ctx context.Context, in *DeleteObjectPoolRequest, opts ...grpc.CallOption) (*DeleteObjectPoolResponse, error)
	// Repositories are assumed to be stored on the same disk
	LinkRepositoryToObjectPool(ctx context.Context, in *LinkRepositoryToObjectPoolRequest, opts ...grpc.CallOption) (*LinkRepositoryToObjectPoolResponse, error)
	// Deprecated: Do not use.
	// UnlinkRepositoryFromObjectPool does not unlink the repository from the
	// object pool as you'd think, but all it really does is to remove the object
	// pool's remote pointing to the repository. And even this is a no-op given
	// that we'd try to remove the remote by the repository's `GlRepository()`
	// name, which we never create in the first place. To unlink repositories
	// from an object pool, you'd really want to execute DisconnectGitAlternates
	// to remove the repository's link to the pool's object database.
	//
	// This function is never called by anyone and highly misleading. It's thus
	// deprecated and will be removed in v14.4.
	UnlinkRepositoryFromObjectPool(ctx context.Context, in *UnlinkRepositoryFromObjectPoolRequest, opts ...grpc.CallOption) (*UnlinkRepositoryFromObjectPoolResponse, error)
	ReduplicateRepository(ctx context.Context, in *ReduplicateRepositoryRequest, opts ...grpc.CallOption) (*ReduplicateRepositoryResponse, error)
	DisconnectGitAlternates(ctx context.Context, in *DisconnectGitAlternatesRequest, opts ...grpc.CallOption) (*DisconnectGitAlternatesResponse, error)
	FetchIntoObjectPool(ctx context.Context, in *FetchIntoObjectPoolRequest, opts ...grpc.CallOption) (*FetchIntoObjectPoolResponse, error)
	GetObjectPool(ctx context.Context, in *GetObjectPoolRequest, opts ...grpc.CallOption) (*GetObjectPoolResponse, error)
}

type objectPoolServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewObjectPoolServiceClient(cc grpc.ClientConnInterface) ObjectPoolServiceClient {
	return &objectPoolServiceClient{cc}
}

func (c *objectPoolServiceClient) CreateObjectPool(ctx context.Context, in *CreateObjectPoolRequest, opts ...grpc.CallOption) (*CreateObjectPoolResponse, error) {
	out := new(CreateObjectPoolResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/CreateObjectPool", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *objectPoolServiceClient) DeleteObjectPool(ctx context.Context, in *DeleteObjectPoolRequest, opts ...grpc.CallOption) (*DeleteObjectPoolResponse, error) {
	out := new(DeleteObjectPoolResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/DeleteObjectPool", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *objectPoolServiceClient) LinkRepositoryToObjectPool(ctx context.Context, in *LinkRepositoryToObjectPoolRequest, opts ...grpc.CallOption) (*LinkRepositoryToObjectPoolResponse, error) {
	out := new(LinkRepositoryToObjectPoolResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/LinkRepositoryToObjectPool", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Deprecated: Do not use.
func (c *objectPoolServiceClient) UnlinkRepositoryFromObjectPool(ctx context.Context, in *UnlinkRepositoryFromObjectPoolRequest, opts ...grpc.CallOption) (*UnlinkRepositoryFromObjectPoolResponse, error) {
	out := new(UnlinkRepositoryFromObjectPoolResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/UnlinkRepositoryFromObjectPool", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *objectPoolServiceClient) ReduplicateRepository(ctx context.Context, in *ReduplicateRepositoryRequest, opts ...grpc.CallOption) (*ReduplicateRepositoryResponse, error) {
	out := new(ReduplicateRepositoryResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/ReduplicateRepository", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *objectPoolServiceClient) DisconnectGitAlternates(ctx context.Context, in *DisconnectGitAlternatesRequest, opts ...grpc.CallOption) (*DisconnectGitAlternatesResponse, error) {
	out := new(DisconnectGitAlternatesResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/DisconnectGitAlternates", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *objectPoolServiceClient) FetchIntoObjectPool(ctx context.Context, in *FetchIntoObjectPoolRequest, opts ...grpc.CallOption) (*FetchIntoObjectPoolResponse, error) {
	out := new(FetchIntoObjectPoolResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/FetchIntoObjectPool", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *objectPoolServiceClient) GetObjectPool(ctx context.Context, in *GetObjectPoolRequest, opts ...grpc.CallOption) (*GetObjectPoolResponse, error) {
	out := new(GetObjectPoolResponse)
	err := c.cc.Invoke(ctx, "/gitaly.ObjectPoolService/GetObjectPool", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ObjectPoolServiceServer is the server API for ObjectPoolService service.
// All implementations must embed UnimplementedObjectPoolServiceServer
// for forward compatibility
type ObjectPoolServiceServer interface {
	CreateObjectPool(context.Context, *CreateObjectPoolRequest) (*CreateObjectPoolResponse, error)
	DeleteObjectPool(context.Context, *DeleteObjectPoolRequest) (*DeleteObjectPoolResponse, error)
	// Repositories are assumed to be stored on the same disk
	LinkRepositoryToObjectPool(context.Context, *LinkRepositoryToObjectPoolRequest) (*LinkRepositoryToObjectPoolResponse, error)
	// Deprecated: Do not use.
	// UnlinkRepositoryFromObjectPool does not unlink the repository from the
	// object pool as you'd think, but all it really does is to remove the object
	// pool's remote pointing to the repository. And even this is a no-op given
	// that we'd try to remove the remote by the repository's `GlRepository()`
	// name, which we never create in the first place. To unlink repositories
	// from an object pool, you'd really want to execute DisconnectGitAlternates
	// to remove the repository's link to the pool's object database.
	//
	// This function is never called by anyone and highly misleading. It's thus
	// deprecated and will be removed in v14.4.
	UnlinkRepositoryFromObjectPool(context.Context, *UnlinkRepositoryFromObjectPoolRequest) (*UnlinkRepositoryFromObjectPoolResponse, error)
	ReduplicateRepository(context.Context, *ReduplicateRepositoryRequest) (*ReduplicateRepositoryResponse, error)
	DisconnectGitAlternates(context.Context, *DisconnectGitAlternatesRequest) (*DisconnectGitAlternatesResponse, error)
	FetchIntoObjectPool(context.Context, *FetchIntoObjectPoolRequest) (*FetchIntoObjectPoolResponse, error)
	GetObjectPool(context.Context, *GetObjectPoolRequest) (*GetObjectPoolResponse, error)
	mustEmbedUnimplementedObjectPoolServiceServer()
}

// UnimplementedObjectPoolServiceServer must be embedded to have forward compatible implementations.
type UnimplementedObjectPoolServiceServer struct {
}

func (UnimplementedObjectPoolServiceServer) CreateObjectPool(context.Context, *CreateObjectPoolRequest) (*CreateObjectPoolResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateObjectPool not implemented")
}
func (UnimplementedObjectPoolServiceServer) DeleteObjectPool(context.Context, *DeleteObjectPoolRequest) (*DeleteObjectPoolResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteObjectPool not implemented")
}
func (UnimplementedObjectPoolServiceServer) LinkRepositoryToObjectPool(context.Context, *LinkRepositoryToObjectPoolRequest) (*LinkRepositoryToObjectPoolResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LinkRepositoryToObjectPool not implemented")
}
func (UnimplementedObjectPoolServiceServer) UnlinkRepositoryFromObjectPool(context.Context, *UnlinkRepositoryFromObjectPoolRequest) (*UnlinkRepositoryFromObjectPoolResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UnlinkRepositoryFromObjectPool not implemented")
}
func (UnimplementedObjectPoolServiceServer) ReduplicateRepository(context.Context, *ReduplicateRepositoryRequest) (*ReduplicateRepositoryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReduplicateRepository not implemented")
}
func (UnimplementedObjectPoolServiceServer) DisconnectGitAlternates(context.Context, *DisconnectGitAlternatesRequest) (*DisconnectGitAlternatesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DisconnectGitAlternates not implemented")
}
func (UnimplementedObjectPoolServiceServer) FetchIntoObjectPool(context.Context, *FetchIntoObjectPoolRequest) (*FetchIntoObjectPoolResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FetchIntoObjectPool not implemented")
}
func (UnimplementedObjectPoolServiceServer) GetObjectPool(context.Context, *GetObjectPoolRequest) (*GetObjectPoolResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetObjectPool not implemented")
}
func (UnimplementedObjectPoolServiceServer) mustEmbedUnimplementedObjectPoolServiceServer() {}

// UnsafeObjectPoolServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ObjectPoolServiceServer will
// result in compilation errors.
type UnsafeObjectPoolServiceServer interface {
	mustEmbedUnimplementedObjectPoolServiceServer()
}

func RegisterObjectPoolServiceServer(s grpc.ServiceRegistrar, srv ObjectPoolServiceServer) {
	s.RegisterService(&ObjectPoolService_ServiceDesc, srv)
}

func _ObjectPoolService_CreateObjectPool_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateObjectPoolRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).CreateObjectPool(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/CreateObjectPool",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).CreateObjectPool(ctx, req.(*CreateObjectPoolRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ObjectPoolService_DeleteObjectPool_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteObjectPoolRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).DeleteObjectPool(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/DeleteObjectPool",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).DeleteObjectPool(ctx, req.(*DeleteObjectPoolRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ObjectPoolService_LinkRepositoryToObjectPool_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LinkRepositoryToObjectPoolRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).LinkRepositoryToObjectPool(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/LinkRepositoryToObjectPool",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).LinkRepositoryToObjectPool(ctx, req.(*LinkRepositoryToObjectPoolRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ObjectPoolService_UnlinkRepositoryFromObjectPool_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnlinkRepositoryFromObjectPoolRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).UnlinkRepositoryFromObjectPool(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/UnlinkRepositoryFromObjectPool",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).UnlinkRepositoryFromObjectPool(ctx, req.(*UnlinkRepositoryFromObjectPoolRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ObjectPoolService_ReduplicateRepository_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReduplicateRepositoryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).ReduplicateRepository(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/ReduplicateRepository",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).ReduplicateRepository(ctx, req.(*ReduplicateRepositoryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ObjectPoolService_DisconnectGitAlternates_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DisconnectGitAlternatesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).DisconnectGitAlternates(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/DisconnectGitAlternates",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).DisconnectGitAlternates(ctx, req.(*DisconnectGitAlternatesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ObjectPoolService_FetchIntoObjectPool_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FetchIntoObjectPoolRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).FetchIntoObjectPool(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/FetchIntoObjectPool",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).FetchIntoObjectPool(ctx, req.(*FetchIntoObjectPoolRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ObjectPoolService_GetObjectPool_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetObjectPoolRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectPoolServiceServer).GetObjectPool(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.ObjectPoolService/GetObjectPool",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectPoolServiceServer).GetObjectPool(ctx, req.(*GetObjectPoolRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ObjectPoolService_ServiceDesc is the grpc.ServiceDesc for ObjectPoolService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ObjectPoolService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.ObjectPoolService",
	HandlerType: (*ObjectPoolServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateObjectPool",
			Handler:    _ObjectPoolService_CreateObjectPool_Handler,
		},
		{
			MethodName: "DeleteObjectPool",
			Handler:    _ObjectPoolService_DeleteObjectPool_Handler,
		},
		{
			MethodName: "LinkRepositoryToObjectPool",
			Handler:    _ObjectPoolService_LinkRepositoryToObjectPool_Handler,
		},
		{
			MethodName: "UnlinkRepositoryFromObjectPool",
			Handler:    _ObjectPoolService_UnlinkRepositoryFromObjectPool_Handler,
		},
		{
			MethodName: "ReduplicateRepository",
			Handler:    _ObjectPoolService_ReduplicateRepository_Handler,
		},
		{
			MethodName: "DisconnectGitAlternates",
			Handler:    _ObjectPoolService_DisconnectGitAlternates_Handler,
		},
		{
			MethodName: "FetchIntoObjectPool",
			Handler:    _ObjectPoolService_FetchIntoObjectPool_Handler,
		},
		{
			MethodName: "GetObjectPool",
			Handler:    _ObjectPoolService_GetObjectPool_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "objectpool.proto",
}
