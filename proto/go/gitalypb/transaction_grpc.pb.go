// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.1
// source: transaction.proto

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

// RefTransactionClient is the client API for RefTransaction service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RefTransactionClient interface {
	// VoteTransaction casts a vote on a transaction to establish whether the
	// node is doing the same change as all the other nodes part of the
	// transaction. This RPC blocks until quorum has been reached, which may be
	// _before_ all nodes have cast a vote.
	//
	// This RPC may return one of the following error codes:
	//
	// - `NotFound` in case the transaction could not be found.
	// - `Canceled` in case the transaction has been canceled before quorum was
	//   reached.
	VoteTransaction(ctx context.Context, in *VoteTransactionRequest, opts ...grpc.CallOption) (*VoteTransactionResponse, error)
	// StopTransaction gracefully stops a transaction. This RPC can be used if
	// only a subset of nodes executes specific code which may cause the
	// transaction to fail. One such example is Git hooks, which only execute on
	// the primary Gitaly noded. Other nodes which vote on this transaction will
	// get a response with the `STOP` state being set.
	//
	// This RPC may return one of the following error codes:
	//
	// - `NotFound` in case the transaction could not be found.
	// - `Canceled` in case the transaction has been canceled before quorum was
	//   reached.
	StopTransaction(ctx context.Context, in *StopTransactionRequest, opts ...grpc.CallOption) (*StopTransactionResponse, error)
}

type refTransactionClient struct {
	cc grpc.ClientConnInterface
}

func NewRefTransactionClient(cc grpc.ClientConnInterface) RefTransactionClient {
	return &refTransactionClient{cc}
}

func (c *refTransactionClient) VoteTransaction(ctx context.Context, in *VoteTransactionRequest, opts ...grpc.CallOption) (*VoteTransactionResponse, error) {
	out := new(VoteTransactionResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefTransaction/VoteTransaction", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *refTransactionClient) StopTransaction(ctx context.Context, in *StopTransactionRequest, opts ...grpc.CallOption) (*StopTransactionResponse, error) {
	out := new(StopTransactionResponse)
	err := c.cc.Invoke(ctx, "/gitaly.RefTransaction/StopTransaction", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RefTransactionServer is the server API for RefTransaction service.
// All implementations must embed UnimplementedRefTransactionServer
// for forward compatibility
type RefTransactionServer interface {
	// VoteTransaction casts a vote on a transaction to establish whether the
	// node is doing the same change as all the other nodes part of the
	// transaction. This RPC blocks until quorum has been reached, which may be
	// _before_ all nodes have cast a vote.
	//
	// This RPC may return one of the following error codes:
	//
	// - `NotFound` in case the transaction could not be found.
	// - `Canceled` in case the transaction has been canceled before quorum was
	//   reached.
	VoteTransaction(context.Context, *VoteTransactionRequest) (*VoteTransactionResponse, error)
	// StopTransaction gracefully stops a transaction. This RPC can be used if
	// only a subset of nodes executes specific code which may cause the
	// transaction to fail. One such example is Git hooks, which only execute on
	// the primary Gitaly noded. Other nodes which vote on this transaction will
	// get a response with the `STOP` state being set.
	//
	// This RPC may return one of the following error codes:
	//
	// - `NotFound` in case the transaction could not be found.
	// - `Canceled` in case the transaction has been canceled before quorum was
	//   reached.
	StopTransaction(context.Context, *StopTransactionRequest) (*StopTransactionResponse, error)
	mustEmbedUnimplementedRefTransactionServer()
}

// UnimplementedRefTransactionServer must be embedded to have forward compatible implementations.
type UnimplementedRefTransactionServer struct {
}

func (UnimplementedRefTransactionServer) VoteTransaction(context.Context, *VoteTransactionRequest) (*VoteTransactionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VoteTransaction not implemented")
}
func (UnimplementedRefTransactionServer) StopTransaction(context.Context, *StopTransactionRequest) (*StopTransactionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StopTransaction not implemented")
}
func (UnimplementedRefTransactionServer) mustEmbedUnimplementedRefTransactionServer() {}

// UnsafeRefTransactionServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RefTransactionServer will
// result in compilation errors.
type UnsafeRefTransactionServer interface {
	mustEmbedUnimplementedRefTransactionServer()
}

func RegisterRefTransactionServer(s grpc.ServiceRegistrar, srv RefTransactionServer) {
	s.RegisterService(&RefTransaction_ServiceDesc, srv)
}

func _RefTransaction_VoteTransaction_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(VoteTransactionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefTransactionServer).VoteTransaction(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefTransaction/VoteTransaction",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefTransactionServer).VoteTransaction(ctx, req.(*VoteTransactionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RefTransaction_StopTransaction_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StopTransactionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefTransactionServer).StopTransaction(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gitaly.RefTransaction/StopTransaction",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefTransactionServer).StopTransaction(ctx, req.(*StopTransactionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RefTransaction_ServiceDesc is the grpc.ServiceDesc for RefTransaction service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RefTransaction_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.RefTransaction",
	HandlerType: (*RefTransactionServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "VoteTransaction",
			Handler:    _RefTransaction_VoteTransaction_Handler,
		},
		{
			MethodName: "StopTransaction",
			Handler:    _RefTransaction_StopTransaction_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "transaction.proto",
}
