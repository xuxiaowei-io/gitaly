// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.17.3
// source: teststream.proto

package gitalypb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TestStreamRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Repository *Repository `protobuf:"bytes,1,opt,name=repository,proto3" json:"repository,omitempty"`
	Size       int64       `protobuf:"varint,2,opt,name=size,proto3" json:"size,omitempty"`
}

func (x *TestStreamRequest) Reset() {
	*x = TestStreamRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_teststream_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TestStreamRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TestStreamRequest) ProtoMessage() {}

func (x *TestStreamRequest) ProtoReflect() protoreflect.Message {
	mi := &file_teststream_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TestStreamRequest.ProtoReflect.Descriptor instead.
func (*TestStreamRequest) Descriptor() ([]byte, []int) {
	return file_teststream_proto_rawDescGZIP(), []int{0}
}

func (x *TestStreamRequest) GetRepository() *Repository {
	if x != nil {
		return x.Repository
	}
	return nil
}

func (x *TestStreamRequest) GetSize() int64 {
	if x != nil {
		return x.Size
	}
	return 0
}

var File_teststream_proto protoreflect.FileDescriptor

var file_teststream_proto_rawDesc = []byte{
	0x0a, 0x10, 0x74, 0x65, 0x73, 0x74, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x06, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x1a, 0x0a, 0x6c, 0x69, 0x6e, 0x74,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0c, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x61, 0x0a, 0x11, 0x54, 0x65, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x38, 0x0a, 0x0a, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69,
	0x74, 0x6f, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x67, 0x69, 0x74,
	0x61, 0x6c, 0x79, 0x2e, 0x52, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x42, 0x04,
	0x98, 0xc6, 0x2c, 0x01, 0x52, 0x0a, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79,
	0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x04,
	0x73, 0x69, 0x7a, 0x65, 0x32, 0x5c, 0x0a, 0x11, 0x54, 0x65, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65,
	0x61, 0x6d, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x47, 0x0a, 0x0a, 0x54, 0x65, 0x73,
	0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x19, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79,
	0x2e, 0x54, 0x65, 0x73, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x06, 0xfa, 0x97, 0x28, 0x02,
	0x08, 0x02, 0x42, 0x34, 0x5a, 0x32, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2d, 0x6f, 0x72, 0x67, 0x2f, 0x67, 0x69, 0x74, 0x61,
	0x6c, 0x79, 0x2f, 0x76, 0x31, 0x34, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67, 0x6f, 0x2f,
	0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_teststream_proto_rawDescOnce sync.Once
	file_teststream_proto_rawDescData = file_teststream_proto_rawDesc
)

func file_teststream_proto_rawDescGZIP() []byte {
	file_teststream_proto_rawDescOnce.Do(func() {
		file_teststream_proto_rawDescData = protoimpl.X.CompressGZIP(file_teststream_proto_rawDescData)
	})
	return file_teststream_proto_rawDescData
}

var file_teststream_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_teststream_proto_goTypes = []interface{}{
	(*TestStreamRequest)(nil), // 0: gitaly.TestStreamRequest
	(*Repository)(nil),        // 1: gitaly.Repository
	(*emptypb.Empty)(nil),     // 2: google.protobuf.Empty
}
var file_teststream_proto_depIdxs = []int32{
	1, // 0: gitaly.TestStreamRequest.repository:type_name -> gitaly.Repository
	0, // 1: gitaly.TestStreamService.TestStream:input_type -> gitaly.TestStreamRequest
	2, // 2: gitaly.TestStreamService.TestStream:output_type -> google.protobuf.Empty
	2, // [2:3] is the sub-list for method output_type
	1, // [1:2] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_teststream_proto_init() }
func file_teststream_proto_init() {
	if File_teststream_proto != nil {
		return
	}
	file_lint_proto_init()
	file_shared_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_teststream_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TestStreamRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_teststream_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_teststream_proto_goTypes,
		DependencyIndexes: file_teststream_proto_depIdxs,
		MessageInfos:      file_teststream_proto_msgTypes,
	}.Build()
	File_teststream_proto = out.File
	file_teststream_proto_rawDesc = nil
	file_teststream_proto_goTypes = nil
	file_teststream_proto_depIdxs = nil
}