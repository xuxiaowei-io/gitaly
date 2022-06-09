// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.1
// source: wiki.proto

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

// WikiServiceClient is the client API for WikiService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type WikiServiceClient interface {
	// This comment is left unintentionally blank.
	WikiWritePage(ctx context.Context, opts ...grpc.CallOption) (WikiService_WikiWritePageClient, error)
	// This comment is left unintentionally blank.
	WikiUpdatePage(ctx context.Context, opts ...grpc.CallOption) (WikiService_WikiUpdatePageClient, error)
	// WikiFindPage returns a stream because the page's raw_data field may be arbitrarily large.
	WikiFindPage(ctx context.Context, in *WikiFindPageRequest, opts ...grpc.CallOption) (WikiService_WikiFindPageClient, error)
	// This comment is left unintentionally blank.
	WikiGetAllPages(ctx context.Context, in *WikiGetAllPagesRequest, opts ...grpc.CallOption) (WikiService_WikiGetAllPagesClient, error)
	// This comment is left unintentionally blank.
	WikiListPages(ctx context.Context, in *WikiListPagesRequest, opts ...grpc.CallOption) (WikiService_WikiListPagesClient, error)
}

type wikiServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewWikiServiceClient(cc grpc.ClientConnInterface) WikiServiceClient {
	return &wikiServiceClient{cc}
}

func (c *wikiServiceClient) WikiWritePage(ctx context.Context, opts ...grpc.CallOption) (WikiService_WikiWritePageClient, error) {
	stream, err := c.cc.NewStream(ctx, &WikiService_ServiceDesc.Streams[0], "/gitaly.WikiService/WikiWritePage", opts...)
	if err != nil {
		return nil, err
	}
	x := &wikiServiceWikiWritePageClient{stream}
	return x, nil
}

type WikiService_WikiWritePageClient interface {
	Send(*WikiWritePageRequest) error
	CloseAndRecv() (*WikiWritePageResponse, error)
	grpc.ClientStream
}

type wikiServiceWikiWritePageClient struct {
	grpc.ClientStream
}

func (x *wikiServiceWikiWritePageClient) Send(m *WikiWritePageRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *wikiServiceWikiWritePageClient) CloseAndRecv() (*WikiWritePageResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(WikiWritePageResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *wikiServiceClient) WikiUpdatePage(ctx context.Context, opts ...grpc.CallOption) (WikiService_WikiUpdatePageClient, error) {
	stream, err := c.cc.NewStream(ctx, &WikiService_ServiceDesc.Streams[1], "/gitaly.WikiService/WikiUpdatePage", opts...)
	if err != nil {
		return nil, err
	}
	x := &wikiServiceWikiUpdatePageClient{stream}
	return x, nil
}

type WikiService_WikiUpdatePageClient interface {
	Send(*WikiUpdatePageRequest) error
	CloseAndRecv() (*WikiUpdatePageResponse, error)
	grpc.ClientStream
}

type wikiServiceWikiUpdatePageClient struct {
	grpc.ClientStream
}

func (x *wikiServiceWikiUpdatePageClient) Send(m *WikiUpdatePageRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *wikiServiceWikiUpdatePageClient) CloseAndRecv() (*WikiUpdatePageResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(WikiUpdatePageResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *wikiServiceClient) WikiFindPage(ctx context.Context, in *WikiFindPageRequest, opts ...grpc.CallOption) (WikiService_WikiFindPageClient, error) {
	stream, err := c.cc.NewStream(ctx, &WikiService_ServiceDesc.Streams[2], "/gitaly.WikiService/WikiFindPage", opts...)
	if err != nil {
		return nil, err
	}
	x := &wikiServiceWikiFindPageClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type WikiService_WikiFindPageClient interface {
	Recv() (*WikiFindPageResponse, error)
	grpc.ClientStream
}

type wikiServiceWikiFindPageClient struct {
	grpc.ClientStream
}

func (x *wikiServiceWikiFindPageClient) Recv() (*WikiFindPageResponse, error) {
	m := new(WikiFindPageResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *wikiServiceClient) WikiGetAllPages(ctx context.Context, in *WikiGetAllPagesRequest, opts ...grpc.CallOption) (WikiService_WikiGetAllPagesClient, error) {
	stream, err := c.cc.NewStream(ctx, &WikiService_ServiceDesc.Streams[3], "/gitaly.WikiService/WikiGetAllPages", opts...)
	if err != nil {
		return nil, err
	}
	x := &wikiServiceWikiGetAllPagesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type WikiService_WikiGetAllPagesClient interface {
	Recv() (*WikiGetAllPagesResponse, error)
	grpc.ClientStream
}

type wikiServiceWikiGetAllPagesClient struct {
	grpc.ClientStream
}

func (x *wikiServiceWikiGetAllPagesClient) Recv() (*WikiGetAllPagesResponse, error) {
	m := new(WikiGetAllPagesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *wikiServiceClient) WikiListPages(ctx context.Context, in *WikiListPagesRequest, opts ...grpc.CallOption) (WikiService_WikiListPagesClient, error) {
	stream, err := c.cc.NewStream(ctx, &WikiService_ServiceDesc.Streams[4], "/gitaly.WikiService/WikiListPages", opts...)
	if err != nil {
		return nil, err
	}
	x := &wikiServiceWikiListPagesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type WikiService_WikiListPagesClient interface {
	Recv() (*WikiListPagesResponse, error)
	grpc.ClientStream
}

type wikiServiceWikiListPagesClient struct {
	grpc.ClientStream
}

func (x *wikiServiceWikiListPagesClient) Recv() (*WikiListPagesResponse, error) {
	m := new(WikiListPagesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// WikiServiceServer is the server API for WikiService service.
// All implementations must embed UnimplementedWikiServiceServer
// for forward compatibility
type WikiServiceServer interface {
	// This comment is left unintentionally blank.
	WikiWritePage(WikiService_WikiWritePageServer) error
	// This comment is left unintentionally blank.
	WikiUpdatePage(WikiService_WikiUpdatePageServer) error
	// WikiFindPage returns a stream because the page's raw_data field may be arbitrarily large.
	WikiFindPage(*WikiFindPageRequest, WikiService_WikiFindPageServer) error
	// This comment is left unintentionally blank.
	WikiGetAllPages(*WikiGetAllPagesRequest, WikiService_WikiGetAllPagesServer) error
	// This comment is left unintentionally blank.
	WikiListPages(*WikiListPagesRequest, WikiService_WikiListPagesServer) error
	mustEmbedUnimplementedWikiServiceServer()
}

// UnimplementedWikiServiceServer must be embedded to have forward compatible implementations.
type UnimplementedWikiServiceServer struct {
}

func (UnimplementedWikiServiceServer) WikiWritePage(WikiService_WikiWritePageServer) error {
	return status.Errorf(codes.Unimplemented, "method WikiWritePage not implemented")
}
func (UnimplementedWikiServiceServer) WikiUpdatePage(WikiService_WikiUpdatePageServer) error {
	return status.Errorf(codes.Unimplemented, "method WikiUpdatePage not implemented")
}
func (UnimplementedWikiServiceServer) WikiFindPage(*WikiFindPageRequest, WikiService_WikiFindPageServer) error {
	return status.Errorf(codes.Unimplemented, "method WikiFindPage not implemented")
}
func (UnimplementedWikiServiceServer) WikiGetAllPages(*WikiGetAllPagesRequest, WikiService_WikiGetAllPagesServer) error {
	return status.Errorf(codes.Unimplemented, "method WikiGetAllPages not implemented")
}
func (UnimplementedWikiServiceServer) WikiListPages(*WikiListPagesRequest, WikiService_WikiListPagesServer) error {
	return status.Errorf(codes.Unimplemented, "method WikiListPages not implemented")
}
func (UnimplementedWikiServiceServer) mustEmbedUnimplementedWikiServiceServer() {}

// UnsafeWikiServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to WikiServiceServer will
// result in compilation errors.
type UnsafeWikiServiceServer interface {
	mustEmbedUnimplementedWikiServiceServer()
}

func RegisterWikiServiceServer(s grpc.ServiceRegistrar, srv WikiServiceServer) {
	s.RegisterService(&WikiService_ServiceDesc, srv)
}

func _WikiService_WikiWritePage_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(WikiServiceServer).WikiWritePage(&wikiServiceWikiWritePageServer{stream})
}

type WikiService_WikiWritePageServer interface {
	SendAndClose(*WikiWritePageResponse) error
	Recv() (*WikiWritePageRequest, error)
	grpc.ServerStream
}

type wikiServiceWikiWritePageServer struct {
	grpc.ServerStream
}

func (x *wikiServiceWikiWritePageServer) SendAndClose(m *WikiWritePageResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *wikiServiceWikiWritePageServer) Recv() (*WikiWritePageRequest, error) {
	m := new(WikiWritePageRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _WikiService_WikiUpdatePage_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(WikiServiceServer).WikiUpdatePage(&wikiServiceWikiUpdatePageServer{stream})
}

type WikiService_WikiUpdatePageServer interface {
	SendAndClose(*WikiUpdatePageResponse) error
	Recv() (*WikiUpdatePageRequest, error)
	grpc.ServerStream
}

type wikiServiceWikiUpdatePageServer struct {
	grpc.ServerStream
}

func (x *wikiServiceWikiUpdatePageServer) SendAndClose(m *WikiUpdatePageResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *wikiServiceWikiUpdatePageServer) Recv() (*WikiUpdatePageRequest, error) {
	m := new(WikiUpdatePageRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _WikiService_WikiFindPage_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WikiFindPageRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WikiServiceServer).WikiFindPage(m, &wikiServiceWikiFindPageServer{stream})
}

type WikiService_WikiFindPageServer interface {
	Send(*WikiFindPageResponse) error
	grpc.ServerStream
}

type wikiServiceWikiFindPageServer struct {
	grpc.ServerStream
}

func (x *wikiServiceWikiFindPageServer) Send(m *WikiFindPageResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _WikiService_WikiGetAllPages_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WikiGetAllPagesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WikiServiceServer).WikiGetAllPages(m, &wikiServiceWikiGetAllPagesServer{stream})
}

type WikiService_WikiGetAllPagesServer interface {
	Send(*WikiGetAllPagesResponse) error
	grpc.ServerStream
}

type wikiServiceWikiGetAllPagesServer struct {
	grpc.ServerStream
}

func (x *wikiServiceWikiGetAllPagesServer) Send(m *WikiGetAllPagesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _WikiService_WikiListPages_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WikiListPagesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WikiServiceServer).WikiListPages(m, &wikiServiceWikiListPagesServer{stream})
}

type WikiService_WikiListPagesServer interface {
	Send(*WikiListPagesResponse) error
	grpc.ServerStream
}

type wikiServiceWikiListPagesServer struct {
	grpc.ServerStream
}

func (x *wikiServiceWikiListPagesServer) Send(m *WikiListPagesResponse) error {
	return x.ServerStream.SendMsg(m)
}

// WikiService_ServiceDesc is the grpc.ServiceDesc for WikiService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var WikiService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.WikiService",
	HandlerType: (*WikiServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "WikiWritePage",
			Handler:       _WikiService_WikiWritePage_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "WikiUpdatePage",
			Handler:       _WikiService_WikiUpdatePage_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "WikiFindPage",
			Handler:       _WikiService_WikiFindPage_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "WikiGetAllPages",
			Handler:       _WikiService_WikiGetAllPages_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "WikiListPages",
			Handler:       _WikiService_WikiListPages_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "wiki.proto",
}
