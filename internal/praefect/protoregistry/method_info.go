package protoregistry

import (
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
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
	targetRepo     []int
	additionalRepo []int
	requestName    string // protobuf message name for input type
	requestFactory protoFactory
	storage        []int
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

// ErrTargetRepoMissing indicates that the target repo is missing or not set
var ErrTargetRepoMissing = errors.New("empty Repository")

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
			return nil, ErrTargetRepoMissing
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
			return nil, ErrTargetRepoMissing
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

	topLevelMsgs, err := getTopLevelMsgs(p)
	if err != nil {
		return MethodInfo{}, err
	}

	typeName, err := lastName(methodDesc.GetInputType())
	if err != nil {
		return MethodInfo{}, err
	}

	if scope == ScopeRepository {
		m := matcher{
			match:        protoutil.GetTargetRepositoryExtension,
			subMatch:     protoutil.GetRepositoryExtension,
			expectedType: ".gitaly.Repository",
			topLevelMsgs: topLevelMsgs,
		}

		targetRepo, err := m.findField(topLevelMsgs[typeName])
		if err != nil {
			return MethodInfo{}, err
		}
		if targetRepo == nil {
			return MethodInfo{}, fmt.Errorf("unable to find target repository for method: %s", requestName)
		}
		mi.targetRepo = targetRepo

		m.match = protoutil.GetAdditionalRepositoryExtension
		additionalRepo, err := m.findField(topLevelMsgs[typeName])
		if err != nil {
			return MethodInfo{}, err
		}
		mi.additionalRepo = additionalRepo
	} else if scope == ScopeStorage {
		m := matcher{
			match:        protoutil.GetStorageExtension,
			topLevelMsgs: topLevelMsgs,
		}
		storage, err := m.findField(topLevelMsgs[typeName])
		if err != nil {
			return MethodInfo{}, err
		}
		if storage == nil {
			return MethodInfo{}, fmt.Errorf("unable to find storage for method: %s", requestName)
		}
		mi.storage = storage
	}

	return mi, nil
}

func getFileTypes(filename string) ([]*descriptorpb.DescriptorProto, error) {
	fd, err := protoreg.GlobalFiles.FindFileByPath(filename)
	if err != nil {
		return nil, err
	}
	sharedFD := protodesc.ToFileDescriptorProto(fd)

	types := sharedFD.GetMessageType()

	for _, dep := range sharedFD.Dependency {
		depTypes, err := getFileTypes(dep)
		if err != nil {
			return nil, err
		}
		types = append(types, depTypes...)
	}

	return types, nil
}

func getTopLevelMsgs(p *descriptorpb.FileDescriptorProto) (map[string]*descriptorpb.DescriptorProto, error) {
	topLevelMsgs := map[string]*descriptorpb.DescriptorProto{}
	types, err := getFileTypes(p.GetName())
	if err != nil {
		return nil, err
	}
	for _, msg := range types {
		topLevelMsgs[msg.GetName()] = msg
	}
	return topLevelMsgs, nil
}

// Matcher helps find field matching credentials. At first match method is used to check fields
// recursively. Then if field matches but type don't match expectedType subMatch method is used
// from this point. This matcher assumes that only one field in the message matches the credentials.
type matcher struct {
	match        func(*descriptorpb.FieldDescriptorProto) (bool, error)
	subMatch     func(*descriptorpb.FieldDescriptorProto) (bool, error)
	expectedType string                                   // fully qualified name of expected type e.g. ".gitaly.Repository"
	topLevelMsgs map[string]*descriptorpb.DescriptorProto // Map of all top level messages in given file and it dependencies. Result of getTopLevelMsgs should be used.
}

func (m matcher) findField(t *descriptorpb.DescriptorProto) ([]int, error) {
	for _, f := range t.GetField() {
		match, err := m.match(f)
		if err != nil {
			return nil, err
		}
		if match {
			if f.GetTypeName() == m.expectedType {
				return []int{int(f.GetNumber())}, nil
			} else if m.subMatch != nil {
				m.match = m.subMatch
				m.subMatch = nil
			} else {
				return nil, fmt.Errorf("found wrong type, expected: %s, got: %s", m.expectedType, f.GetTypeName())
			}
		}

		childMsg, err := findChildMsg(m.topLevelMsgs, t, f)
		if err != nil {
			return nil, err
		}

		if childMsg != nil {
			nestedField, err := m.findField(childMsg)
			if err != nil {
				return nil, err
			}
			if nestedField != nil {
				return append([]int{int(f.GetNumber())}, nestedField...), nil
			}
		}
	}
	return nil, nil
}

func findChildMsg(topLevelMsgs map[string]*descriptorpb.DescriptorProto, t *descriptorpb.DescriptorProto, f *descriptorpb.FieldDescriptorProto) (*descriptorpb.DescriptorProto, error) {
	var childType *descriptorpb.DescriptorProto
	const msgPrimitive = "TYPE_MESSAGE"
	if primitive := f.GetType().String(); primitive != msgPrimitive {
		return nil, nil
	}

	msgName, err := lastName(f.GetTypeName())
	if err != nil {
		return nil, err
	}

	for _, nestedType := range t.GetNestedType() {
		if msgName == nestedType.GetName() {
			return nestedType, nil
		}
	}

	if childType = topLevelMsgs[msgName]; childType != nil {
		return childType, nil
	}

	return nil, fmt.Errorf("could not find message type %q", msgName)
}

func lastName(inputType string) (string, error) {
	tokens := strings.Split(inputType, ".")

	msgName := tokens[len(tokens)-1]
	if msgName == "" {
		return "", fmt.Errorf("unable to parse method input type: %s", inputType)
	}

	return msgName, nil
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
