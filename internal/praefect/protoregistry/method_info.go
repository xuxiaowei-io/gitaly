package protoregistry

import (
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	protoreg "google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// OpType represents the operation type for a RPC method
type OpType int

const (
	// OpUnknown = unknown operation type
	OpUnknown OpType = iota
	// OpAccessor = accessor operation type (ready only)
	OpAccessor
	// OpMutator = mutator operation type (modifies a repository)
	OpMutator
	// OpMaintenance is an operation which performs maintenance-tasks on the repository. It
	// shouldn't ever result in a user-visible change in behaviour, except that it may repair
	// corrupt data.
	OpMaintenance
)

// Scope represents the intended scope of an RPC method
type Scope int

const (
	// ScopeUnknown is the default scope until determined otherwise
	ScopeUnknown Scope = iota
	// ScopeRepository indicates an RPC's scope is limited to a repository
	ScopeRepository
	// ScopeStorage indicates an RPC is scoped to an entire storage location
	ScopeStorage
)

func (s Scope) String() string {
	switch s {
	case ScopeStorage:
		return "storage"
	case ScopeRepository:
		return "repository"
	default:
		return fmt.Sprintf("N/A: %d", s)
	}
}

var protoScope = map[gitalypb.OperationMsg_Scope]Scope{
	gitalypb.OperationMsg_REPOSITORY: ScopeRepository,
	gitalypb.OperationMsg_STORAGE:    ScopeStorage,
}

// MethodInfo contains metadata about the RPC method. Refer to documentation
// for message type "OperationMsg" shared.proto in ./proto for
// more documentation.
type MethodInfo struct {
	Operation      OpType
	Scope          Scope
	requestName    string // protobuf message name for input type
	requestFactory protoFactory
	fullMethodName string
}

// TargetRepo returns the target repository for a protobuf message if it exists
func (mi MethodInfo) TargetRepo(msg proto.Message) (*gitalypb.Repository, error) {
	return mi.getRepo(msg, gitalypb.E_TargetRepository)
}

// AdditionalRepo returns the additional repository for a Protobuf message that needs a storage
// rewritten if it exists.
func (mi MethodInfo) AdditionalRepo(msg proto.Message) (*gitalypb.Repository, error) {
	return mi.getRepo(msg, gitalypb.E_AdditionalRepository)
}

//nolint:revive // This is unintentionally missing documentation.
func (mi MethodInfo) FullMethodName() string {
	return mi.fullMethodName
}

// ErrRepositoryFieldNotFound indicates that the repository field could not be found.
var ErrRepositoryFieldNotFound = errors.New("repository field not found")

func (mi MethodInfo) getRepo(msg proto.Message, extensionType protoreflect.ExtensionType) (*gitalypb.Repository, error) {
	if mi.requestName != string(proto.MessageName(msg)) {
		return nil, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	field, err := findFieldByExtension(msg, extensionType)
	if err != nil {
		if errors.Is(err, errFieldNotFound) {
			return nil, ErrRepositoryFieldNotFound
		}

		return nil, err
	}

	if field.desc.Kind() != protoreflect.MessageKind {
		return nil, fmt.Errorf("expected repository message, got %s", field.desc.Kind().String())
	}

	switch fieldMsg := field.value.Message().Interface().(type) {
	case *gitalypb.Repository:
		return fieldMsg, nil
	case *gitalypb.ObjectPool:
		repo := fieldMsg.GetRepository()
		if repo == nil {
			return nil, ErrRepositoryFieldNotFound
		}

		return repo, nil
	default:
		return nil, fmt.Errorf("repository message has unexpected type %T", fieldMsg)
	}
}

// Storage returns the storage name for a protobuf message if it exists
func (mi MethodInfo) Storage(msg proto.Message) (string, error) {
	field, err := mi.getStorageField(msg)
	if err != nil {
		return "", err
	}

	return field.value.String(), nil
}

// SetStorage sets the storage name for a protobuf message
func (mi MethodInfo) SetStorage(msg proto.Message, storage string) error {
	field, err := mi.getStorageField(msg)
	if err != nil {
		return err
	}

	msg.ProtoReflect().Set(field.desc, protoreflect.ValueOfString(storage))

	return nil
}

func (mi MethodInfo) getStorageField(msg proto.Message) (valueField, error) {
	if mi.requestName != string(proto.MessageName(msg)) {
		return valueField{}, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	field, err := findFieldByExtension(msg, gitalypb.E_Storage)
	if err != nil {
		if errors.Is(err, errFieldNotFound) {
			return valueField{}, fmt.Errorf("target storage field not found")
		}
		return valueField{}, err
	}

	if field.desc.Kind() != protoreflect.StringKind {
		return valueField{}, fmt.Errorf("expected string, got %s", field.desc.Kind().String())
	}

	return field, nil
}

// UnmarshalRequestProto will unmarshal the bytes into the method's request
// message type
func (mi MethodInfo) UnmarshalRequestProto(b []byte) (proto.Message, error) {
	return mi.requestFactory(b)
}

type protoFactory func([]byte) (proto.Message, error)

func methodReqFactory(method *descriptorpb.MethodDescriptorProto) (protoFactory, error) {
	// for some reason, the descriptor prepends a dot not expected in Go
	inputTypeName := strings.TrimPrefix(method.GetInputType(), ".")

	inputType, err := protoreg.GlobalTypes.FindMessageByName(protoreflect.FullName(inputTypeName))
	if err != nil {
		return nil, fmt.Errorf("no message type found for %w", err)
	}

	f := func(buf []byte) (proto.Message, error) {
		pb := inputType.New().Interface()
		if err := proto.Unmarshal(buf, pb); err != nil {
			return nil, err
		}

		return pb, nil
	}

	return f, nil
}

func parseMethodInfo(
	p *descriptorpb.FileDescriptorProto,
	methodDesc *descriptorpb.MethodDescriptorProto,
	fullMethodName string,
) (MethodInfo, error) {
	opMsg, err := protoutil.GetOpExtension(methodDesc)
	if err != nil {
		return MethodInfo{}, err
	}

	var opCode OpType

	switch opMsg.GetOp() {
	case gitalypb.OperationMsg_ACCESSOR:
		opCode = OpAccessor
	case gitalypb.OperationMsg_MUTATOR:
		opCode = OpMutator
	case gitalypb.OperationMsg_MAINTENANCE:
		opCode = OpMaintenance
	default:
		opCode = OpUnknown
	}

	// for some reason, the protobuf descriptor contains an extra dot in front
	// of the request name that the generated code does not. This trimming keeps
	// the two copies consistent for comparisons.
	requestName := strings.TrimLeft(methodDesc.GetInputType(), ".")

	reqFactory, err := methodReqFactory(methodDesc)
	if err != nil {
		return MethodInfo{}, err
	}

	scope, ok := protoScope[opMsg.GetScopeLevel()]
	if !ok {
		return MethodInfo{}, fmt.Errorf("encountered unknown method scope %d", opMsg.GetScopeLevel())
	}

	mi := MethodInfo{
		Operation:      opCode,
		Scope:          scope,
		requestName:    requestName,
		requestFactory: reqFactory,
		fullMethodName: fullMethodName,
	}

	return mi, nil
}

type valueField struct {
	desc  protoreflect.FieldDescriptor
	value protoreflect.Value
}

// findFieldsByExtension will search through all populated fields and returns all of those which
// have the given extension type set.
func findFieldsByExtension(msg proto.Message, extensionType protoreflect.ExtensionType) ([]valueField, error) {
	var valueFields []valueField

	if err := (protorange.Options{Stable: true}).Range(msg.ProtoReflect(), func(values protopath.Values) error {
		value := values.Index(-1)

		fieldDescriptor := value.Step.FieldDescriptor()
		if fieldDescriptor == nil {
			return nil
		}

		opts := fieldDescriptor.Options().(*descriptorpb.FieldOptions)
		if !proto.HasExtension(opts, extensionType) {
			return nil
		}

		valueFields = append(valueFields, valueField{
			desc:  fieldDescriptor,
			value: value.Value,
		})

		return nil
	}, nil); err != nil {
		return nil, fmt.Errorf("ranging over message: %w", err)
	}

	return valueFields, nil
}

var (
	errFieldNotFound  = errors.New("field not found")
	errFieldAmbiguous = errors.New("field is ambiguous")
)

// findFieldByExtension is a wrapper around findFieldsByExtension that returns a single field
// descriptor, only. Returns a errFieldNotFound error in case the field wasn't found, and a
// errFieldAmbiguous error in case there are multiple fields with the same extension.
func findFieldByExtension(msg proto.Message, extensionType protoreflect.ExtensionType) (valueField, error) {
	fields, err := findFieldsByExtension(msg, extensionType)
	if err != nil {
		return valueField{}, err
	}

	switch len(fields) {
	case 1:
		return fields[0], nil
	case 0:
		return valueField{}, errFieldNotFound
	default:
		return valueField{}, errFieldAmbiguous
	}
}
