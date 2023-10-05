// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.1
// source: lint.proto

package gitalypb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Operation ...
type OperationMsg_Operation int32

const (
	// UNKNOWN ...
	OperationMsg_UNKNOWN OperationMsg_Operation = 0 // protolint:disable:this ENUM_FIELD_NAMES_PREFIX ENUM_FIELD_NAMES_ZERO_VALUE_END_WITH
	// MUTATOR ...
	OperationMsg_MUTATOR OperationMsg_Operation = 1 // protolint:disable:this ENUM_FIELD_NAMES_PREFIX
	// ACCESSOR ...
	OperationMsg_ACCESSOR OperationMsg_Operation = 2 // protolint:disable:this ENUM_FIELD_NAMES_PREFIX
	// MAINTENANCE ...
	OperationMsg_MAINTENANCE OperationMsg_Operation = 3 // protolint:disable:this ENUM_FIELD_NAMES_PREFIX
)

// Enum value maps for OperationMsg_Operation.
var (
	OperationMsg_Operation_name = map[int32]string{
		0: "UNKNOWN",
		1: "MUTATOR",
		2: "ACCESSOR",
		3: "MAINTENANCE",
	}
	OperationMsg_Operation_value = map[string]int32{
		"UNKNOWN":     0,
		"MUTATOR":     1,
		"ACCESSOR":    2,
		"MAINTENANCE": 3,
	}
)

func (x OperationMsg_Operation) Enum() *OperationMsg_Operation {
	p := new(OperationMsg_Operation)
	*p = x
	return p
}

func (x OperationMsg_Operation) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (OperationMsg_Operation) Descriptor() protoreflect.EnumDescriptor {
	return file_lint_proto_enumTypes[0].Descriptor()
}

func (OperationMsg_Operation) Type() protoreflect.EnumType {
	return &file_lint_proto_enumTypes[0]
}

func (x OperationMsg_Operation) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use OperationMsg_Operation.Descriptor instead.
func (OperationMsg_Operation) EnumDescriptor() ([]byte, []int) {
	return file_lint_proto_rawDescGZIP(), []int{0, 0}
}

// Scope ...
type OperationMsg_Scope int32

const (
	// REPOSITORY ...
	OperationMsg_REPOSITORY OperationMsg_Scope = 0 // protolint:disable:this ENUM_FIELD_NAMES_PREFIX ENUM_FIELD_NAMES_ZERO_VALUE_END_WITH
	// STORAGE ...
	OperationMsg_STORAGE OperationMsg_Scope = 2 // protolint:disable:this ENUM_FIELD_NAMES_PREFIX
)

// Enum value maps for OperationMsg_Scope.
var (
	OperationMsg_Scope_name = map[int32]string{
		0: "REPOSITORY",
		2: "STORAGE",
	}
	OperationMsg_Scope_value = map[string]int32{
		"REPOSITORY": 0,
		"STORAGE":    2,
	}
)

func (x OperationMsg_Scope) Enum() *OperationMsg_Scope {
	p := new(OperationMsg_Scope)
	*p = x
	return p
}

func (x OperationMsg_Scope) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (OperationMsg_Scope) Descriptor() protoreflect.EnumDescriptor {
	return file_lint_proto_enumTypes[1].Descriptor()
}

func (OperationMsg_Scope) Type() protoreflect.EnumType {
	return &file_lint_proto_enumTypes[1]
}

func (x OperationMsg_Scope) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use OperationMsg_Scope.Descriptor instead.
func (OperationMsg_Scope) EnumDescriptor() ([]byte, []int) {
	return file_lint_proto_rawDescGZIP(), []int{0, 1}
}

// OperationMsg ...
type OperationMsg struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// op ...
	Op OperationMsg_Operation `protobuf:"varint,1,opt,name=op,proto3,enum=gitaly.OperationMsg_Operation" json:"op,omitempty"`
	// scope_level indicates what level an RPC interacts with a server:
	//   - REPOSITORY: scoped to only a single repo
	//   - SERVER: affects the entire server and potentially all repos
	//   - STORAGE: scoped to a specific storage location and all repos within
	ScopeLevel OperationMsg_Scope `protobuf:"varint,2,opt,name=scope_level,json=scopeLevel,proto3,enum=gitaly.OperationMsg_Scope" json:"scope_level,omitempty"`
}

func (x *OperationMsg) Reset() {
	*x = OperationMsg{}
	if protoimpl.UnsafeEnabled {
		mi := &file_lint_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OperationMsg) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OperationMsg) ProtoMessage() {}

func (x *OperationMsg) ProtoReflect() protoreflect.Message {
	mi := &file_lint_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OperationMsg.ProtoReflect.Descriptor instead.
func (*OperationMsg) Descriptor() ([]byte, []int) {
	return file_lint_proto_rawDescGZIP(), []int{0}
}

func (x *OperationMsg) GetOp() OperationMsg_Operation {
	if x != nil {
		return x.Op
	}
	return OperationMsg_UNKNOWN
}

func (x *OperationMsg) GetScopeLevel() OperationMsg_Scope {
	if x != nil {
		return x.ScopeLevel
	}
	return OperationMsg_REPOSITORY
}

var file_lint_proto_extTypes = []protoimpl.ExtensionInfo{
	{
		ExtendedType:  (*descriptorpb.ServiceOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         82302,
		Name:          "gitaly.intercepted",
		Tag:           "varint,82302,opt,name=intercepted",
		Filename:      "lint.proto",
	},
	{
		ExtendedType:  (*descriptorpb.MethodOptions)(nil),
		ExtensionType: (*OperationMsg)(nil),
		Field:         82303,
		Name:          "gitaly.op_type",
		Tag:           "bytes,82303,opt,name=op_type",
		Filename:      "lint.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         91233,
		Name:          "gitaly.storage",
		Tag:           "varint,91233,opt,name=storage",
		Filename:      "lint.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         91234,
		Name:          "gitaly.repository",
		Tag:           "varint,91234,opt,name=repository",
		Filename:      "lint.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         91235,
		Name:          "gitaly.target_repository",
		Tag:           "varint,91235,opt,name=target_repository",
		Filename:      "lint.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         91236,
		Name:          "gitaly.additional_repository",
		Tag:           "varint,91236,opt,name=additional_repository",
		Filename:      "lint.proto",
	},
}

// Extension fields to descriptorpb.ServiceOptions.
var (
	// intercepted indicates whether the proxy intercepts and handles the call
	// instead of proxying. Intercepted services do not require scope or operation
	// annotations.
	//
	// optional bool intercepted = 82302;
	E_Intercepted = &file_lint_proto_extTypes[0]
)

// Extension fields to descriptorpb.MethodOptions.
var (
	// op_type ...
	//
	// optional gitaly.OperationMsg op_type = 82303;
	E_OpType = &file_lint_proto_extTypes[1] // Random high number..
)

// Extension fields to descriptorpb.FieldOptions.
var (
	// storage is used to mark field containing name of affected storage.
	//
	// optional bool storage = 91233;
	E_Storage = &file_lint_proto_extTypes[2] // Random high number..
	// repository annotation is used mark field used as repository
	// when parent message is marked as target or additional repository
	// If this operation modifies a repository, this annotations
	// will specify the location of the Repository field within
	// the request message.
	//
	// optional bool repository = 91234;
	E_Repository = &file_lint_proto_extTypes[3]
	// target_repository is used to mark target repository.
	//
	// optional bool target_repository = 91235;
	E_TargetRepository = &file_lint_proto_extTypes[4]
	// additional_repository is used to mark additional repository.
	//
	// optional bool additional_repository = 91236;
	E_AdditionalRepository = &file_lint_proto_extTypes[5]
)

var File_lint_proto protoreflect.FileDescriptor

var file_lint_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x6c, 0x69, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x67, 0x69,
	0x74, 0x61, 0x6c, 0x79, 0x1a, 0x20, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xf5, 0x01, 0x0a, 0x0c, 0x4f, 0x70, 0x65, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x73, 0x67, 0x12, 0x2e, 0x0a, 0x02, 0x6f, 0x70, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0e, 0x32, 0x1e, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x4f, 0x70, 0x65,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x73, 0x67, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x52, 0x02, 0x6f, 0x70, 0x12, 0x3b, 0x0a, 0x0b, 0x73, 0x63, 0x6f, 0x70, 0x65,
	0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1a, 0x2e, 0x67,
	0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4d,
	0x73, 0x67, 0x2e, 0x53, 0x63, 0x6f, 0x70, 0x65, 0x52, 0x0a, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x4c,
	0x65, 0x76, 0x65, 0x6c, 0x22, 0x44, 0x0a, 0x09, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x0b, 0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x0b,
	0x0a, 0x07, 0x4d, 0x55, 0x54, 0x41, 0x54, 0x4f, 0x52, 0x10, 0x01, 0x12, 0x0c, 0x0a, 0x08, 0x41,
	0x43, 0x43, 0x45, 0x53, 0x53, 0x4f, 0x52, 0x10, 0x02, 0x12, 0x0f, 0x0a, 0x0b, 0x4d, 0x41, 0x49,
	0x4e, 0x54, 0x45, 0x4e, 0x41, 0x4e, 0x43, 0x45, 0x10, 0x03, 0x22, 0x32, 0x0a, 0x05, 0x53, 0x63,
	0x6f, 0x70, 0x65, 0x12, 0x0e, 0x0a, 0x0a, 0x52, 0x45, 0x50, 0x4f, 0x53, 0x49, 0x54, 0x4f, 0x52,
	0x59, 0x10, 0x00, 0x12, 0x0b, 0x0a, 0x07, 0x53, 0x54, 0x4f, 0x52, 0x41, 0x47, 0x45, 0x10, 0x02,
	0x22, 0x04, 0x08, 0x01, 0x10, 0x01, 0x2a, 0x06, 0x53, 0x45, 0x52, 0x56, 0x45, 0x52, 0x3a, 0x43,
	0x0a, 0x0b, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x63, 0x65, 0x70, 0x74, 0x65, 0x64, 0x12, 0x1f, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xfe,
	0x82, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0b, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x63, 0x65, 0x70,
	0x74, 0x65, 0x64, 0x3a, 0x4f, 0x0a, 0x07, 0x6f, 0x70, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x12, 0x1e,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xff,
	0x82, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x2e,
	0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x73, 0x67, 0x52, 0x06, 0x6f, 0x70,
	0x54, 0x79, 0x70, 0x65, 0x3a, 0x39, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x12,
	0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xe1,
	0xc8, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x3a,
	0x3f, 0x0a, 0x0a, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x12, 0x1d, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xe2, 0xc8, 0x05,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x72, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79,
	0x3a, 0x4c, 0x0a, 0x11, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x72, 0x65, 0x70, 0x6f, 0x73,
	0x69, 0x74, 0x6f, 0x72, 0x79, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x18, 0xe3, 0xc8, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x10, 0x74, 0x61,
	0x72, 0x67, 0x65, 0x74, 0x52, 0x65, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x3a, 0x54,
	0x0a, 0x15, 0x61, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x5f, 0x72, 0x65, 0x70,
	0x6f, 0x73, 0x69, 0x74, 0x6f, 0x72, 0x79, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xe4, 0xc8, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x14,
	0x61, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x52, 0x65, 0x70, 0x6f, 0x73, 0x69,
	0x74, 0x6f, 0x72, 0x79, 0x42, 0x34, 0x5a, 0x32, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2d, 0x6f, 0x72, 0x67, 0x2f, 0x67, 0x69,
	0x74, 0x61, 0x6c, 0x79, 0x2f, 0x76, 0x31, 0x36, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67,
	0x6f, 0x2f, 0x67, 0x69, 0x74, 0x61, 0x6c, 0x79, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_lint_proto_rawDescOnce sync.Once
	file_lint_proto_rawDescData = file_lint_proto_rawDesc
)

func file_lint_proto_rawDescGZIP() []byte {
	file_lint_proto_rawDescOnce.Do(func() {
		file_lint_proto_rawDescData = protoimpl.X.CompressGZIP(file_lint_proto_rawDescData)
	})
	return file_lint_proto_rawDescData
}

var file_lint_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_lint_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_lint_proto_goTypes = []interface{}{
	(OperationMsg_Operation)(0),         // 0: gitaly.OperationMsg.Operation
	(OperationMsg_Scope)(0),             // 1: gitaly.OperationMsg.Scope
	(*OperationMsg)(nil),                // 2: gitaly.OperationMsg
	(*descriptorpb.ServiceOptions)(nil), // 3: google.protobuf.ServiceOptions
	(*descriptorpb.MethodOptions)(nil),  // 4: google.protobuf.MethodOptions
	(*descriptorpb.FieldOptions)(nil),   // 5: google.protobuf.FieldOptions
}
var file_lint_proto_depIdxs = []int32{
	0, // 0: gitaly.OperationMsg.op:type_name -> gitaly.OperationMsg.Operation
	1, // 1: gitaly.OperationMsg.scope_level:type_name -> gitaly.OperationMsg.Scope
	3, // 2: gitaly.intercepted:extendee -> google.protobuf.ServiceOptions
	4, // 3: gitaly.op_type:extendee -> google.protobuf.MethodOptions
	5, // 4: gitaly.storage:extendee -> google.protobuf.FieldOptions
	5, // 5: gitaly.repository:extendee -> google.protobuf.FieldOptions
	5, // 6: gitaly.target_repository:extendee -> google.protobuf.FieldOptions
	5, // 7: gitaly.additional_repository:extendee -> google.protobuf.FieldOptions
	2, // 8: gitaly.op_type:type_name -> gitaly.OperationMsg
	9, // [9:9] is the sub-list for method output_type
	9, // [9:9] is the sub-list for method input_type
	8, // [8:9] is the sub-list for extension type_name
	2, // [2:8] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_lint_proto_init() }
func file_lint_proto_init() {
	if File_lint_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_lint_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OperationMsg); i {
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
			RawDescriptor: file_lint_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   1,
			NumExtensions: 6,
			NumServices:   0,
		},
		GoTypes:           file_lint_proto_goTypes,
		DependencyIndexes: file_lint_proto_depIdxs,
		EnumInfos:         file_lint_proto_enumTypes,
		MessageInfos:      file_lint_proto_msgTypes,
		ExtensionInfos:    file_lint_proto_extTypes,
	}.Build()
	File_lint_proto = out.File
	file_lint_proto_rawDesc = nil
	file_lint_proto_goTypes = nil
	file_lint_proto_depIdxs = nil
}
