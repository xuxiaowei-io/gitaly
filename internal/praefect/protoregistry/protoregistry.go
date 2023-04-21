package protoregistry

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/reflect/protodesc"
	protoreg "google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// GitalyProtoPreregistered is a proto registry pre-registered with all
// gitalypb.GitalyProtos proto files.
var GitalyProtoPreregistered *Registry

func init() {
	var err error
	GitalyProtoPreregistered, err = NewFromPaths(gitalypb.GitalyProtos...)
	if err != nil {
		panic(err)
	}
}

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
	return mi.getRepo(msg, mi.targetRepo)
}

// AdditionalRepo returns the additional repository for a protobuf message that needs a storage rewritten
// if it exists
func (mi MethodInfo) AdditionalRepo(msg proto.Message) (*gitalypb.Repository, bool, error) {
	if mi.additionalRepo == nil {
		return nil, false, nil
	}

	repo, err := mi.getRepo(msg, mi.additionalRepo)

	return repo, true, err
}

//nolint:revive // This is unintentionally missing documentation.
func (mi MethodInfo) FullMethodName() string {
	return mi.fullMethodName
}

// ServiceNameAndMethodName returns a tuple of service name and method name. The service name
// includes its package name.
func (mi MethodInfo) ServiceNameAndMethodName() (string, string) {
	parts := strings.SplitN(strings.TrimPrefix(mi.fullMethodName, "/"), "/", 2)
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func (mi MethodInfo) getRepo(msg proto.Message, targetOid []int) (*gitalypb.Repository, error) {
	if mi.requestName != string(proto.MessageName(msg)) {
		return nil, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	repo, err := reflectFindRepoTarget(msg, targetOid)
	switch {
	case err != nil:
		return nil, err
	case repo == nil:
		// it is possible for the target repo to not be set (especially in our unit
		// tests designed to fail and this should return an error to prevent nil
		// pointer dereferencing
		return nil, ErrTargetRepoMissing
	default:
		return repo, nil
	}
}

// Storage returns the storage name for a protobuf message if it exists
func (mi MethodInfo) Storage(msg proto.Message) (string, error) {
	if mi.requestName != string(proto.MessageName(msg)) {
		return "", fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	return reflectFindStorage(msg, mi.storage)
}

// SetStorage sets the storage name for a protobuf message
func (mi MethodInfo) SetStorage(msg proto.Message, storage string) error {
	if mi.requestName != string(proto.MessageName(msg)) {
		return fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	return reflectSetStorage(msg, mi.storage, storage)
}

// UnmarshalRequestProto will unmarshal the bytes into the method's request
// message type
func (mi MethodInfo) UnmarshalRequestProto(b []byte) (proto.Message, error) {
	return mi.requestFactory(b)
}

// Registry contains info about RPC methods
type Registry struct {
	protos map[string]MethodInfo
	// interceptedMethods contains the set of methods which are intercepted
	// by Praefect instead of proxying.
	interceptedMethods map[string]struct{}
}

// New creates a new ProtoRegistry with info from one or more descriptor.FileDescriptorProto
func New(protos ...*descriptorpb.FileDescriptorProto) (*Registry, error) {
	methods := make(map[string]MethodInfo)
	interceptedMethods := make(map[string]struct{})

	for _, p := range protos {
		for _, svc := range p.GetService() {
			for _, method := range svc.GetMethod() {
				fullMethodName := fmt.Sprintf("/%s.%s/%s",
					p.GetPackage(), svc.GetName(), method.GetName(),
				)

				if intercepted, err := protoutil.IsInterceptedMethod(svc, method); err != nil {
					return nil, fmt.Errorf("is intercepted: %w", err)
				} else if intercepted {
					interceptedMethods[fullMethodName] = struct{}{}
					continue
				}

				mi, err := parseMethodInfo(p, method, fullMethodName)
				if err != nil {
					return nil, err
				}

				methods[fullMethodName] = mi
			}
		}
	}

	return &Registry{
		protos:             methods,
		interceptedMethods: interceptedMethods,
	}, nil
}

// NewFromPaths returns a new Registry, initialized with the contents
// of the provided files.
func NewFromPaths(paths ...string) (*Registry, error) {
	fds := make([]*descriptorpb.FileDescriptorProto, 0, len(paths))
	for _, path := range paths {
		fd, err := protoreg.GlobalFiles.FindFileByPath(path)
		if err != nil {
			return nil, err
		}
		fds = append(fds, protodesc.ToFileDescriptorProto(fd))
	}
	return New(fds...)
}

// LookupMethod looks up an MethodInfo by service and method name
func (pr *Registry) LookupMethod(fullMethodName string) (MethodInfo, error) {
	methodInfo, ok := pr.protos[fullMethodName]
	if !ok {
		return MethodInfo{}, fmt.Errorf("full method name not found: %v", fullMethodName)
	}
	return methodInfo, nil
}

// Methods returns all registered methods
func (pr *Registry) Methods() []MethodInfo {
	methods := make([]MethodInfo, 0, len(pr.protos))
	for _, proto := range pr.protos {
		methods = append(methods, proto)
	}
	return methods
}

// IsInterceptedMethod returns whether Praefect intercepts the method call instead of proxying it.
func (pr *Registry) IsInterceptedMethod(fullMethodName string) bool {
	_, ok := pr.interceptedMethods[fullMethodName]
	return ok
}
