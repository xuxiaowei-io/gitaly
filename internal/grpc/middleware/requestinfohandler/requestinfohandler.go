package requestinfohandler

import (
	"context"
	"strings"

	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var requests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_service_client_requests_total",
		Help: "Counter of client requests received by client, call_site, auth version, response code and deadline_type",
	},
	[]string{
		"client_name",
		"grpc_service",
		"grpc_method",
		"call_site",
		"auth_version",
		"grpc_code",
		"deadline_type",
		"method_operation",
		"method_scope",
	},
)

type requestInfo struct {
	correlationID   string
	fullMethod      string
	methodType      string
	clientName      string
	remoteIP        string
	userID          string
	userName        string
	callSite        string
	authVersion     string
	deadlineType    string
	methodOperation string
	methodScope     string
}

// Unknown client and feature. Matches the prometheus grpc unknown value
const unknownValue = "unknown"

func getFromMD(md metadata.MD, header string) string {
	values := md[header]
	if len(values) != 1 {
		return ""
	}

	return values[0]
}

// newRequestInfo extracts metadata from the connection headers and add it to the
// ctx_tags, if it is set. Returns values appropriate for use with prometheus labels,
// using `unknown` if a value is not set
func newRequestInfo(ctx context.Context, fullMethod, grpcMethodType string) requestInfo {
	info := requestInfo{
		fullMethod:      fullMethod,
		methodType:      grpcMethodType,
		clientName:      unknownValue,
		callSite:        unknownValue,
		authVersion:     unknownValue,
		deadlineType:    unknownValue,
		methodOperation: unknownValue,
		methodScope:     unknownValue,
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return info
	}

	if methodInfo, err := protoregistry.GitalyProtoPreregistered.LookupMethod(fullMethod); err == nil {
		var operation string
		switch methodInfo.Operation {
		case protoregistry.OpAccessor:
			operation = "accessor"
		case protoregistry.OpMutator:
			operation = "mutator"
		case protoregistry.OpMaintenance:
			operation = "maintenance"
		default:
			operation = unknownValue
		}

		info.methodOperation = operation

		var scope string
		switch methodInfo.Scope {
		case protoregistry.ScopeRepository:
			scope = "repository"
		case protoregistry.ScopeStorage:
			scope = "storage"
		default:
			scope = unknownValue
		}

		info.methodScope = scope
	}

	if callSite := getFromMD(md, "call_site"); callSite != "" {
		info.callSite = callSite
	}

	if _, deadlineSet := ctx.Deadline(); !deadlineSet {
		info.deadlineType = "none"
	} else if deadlineType := getFromMD(md, "deadline_type"); deadlineType != "" {
		info.deadlineType = deadlineType
	}

	if clientName := correlation.ExtractClientNameFromContext(ctx); clientName != "" {
		info.clientName = clientName
	} else if clientName := getFromMD(md, "client_name"); clientName != "" {
		info.clientName = clientName
	}

	if authInfo, _ := gitalyauth.ExtractAuthInfo(ctx); authInfo != nil {
		info.authVersion = authInfo.Version
	}

	if remoteIP := getFromMD(md, "remote_ip"); remoteIP != "" {
		info.remoteIP = remoteIP
	}

	if userID := getFromMD(md, "user_id"); userID != "" {
		info.userID = userID
	}

	if userName := getFromMD(md, "username"); userName != "" {
		info.userName = userName
	}

	// This is a stop-gap approach to logging correlation_ids
	if correlationID := correlation.ExtractFromContext(ctx); correlationID != "" {
		info.correlationID = correlationID
	}

	return info
}

func (i requestInfo) injectTags(ctx context.Context) {
	tags := grpcmwtags.Extract(ctx)

	for key, value := range map[string]string{
		"grpc.meta.call_site":        i.callSite,
		"grpc.meta.client_name":      i.clientName,
		"grpc.meta.auth_version":     i.authVersion,
		"grpc.meta.deadline_type":    i.deadlineType,
		"grpc.meta.method_type":      i.methodType,
		"grpc.meta.method_operation": i.methodOperation,
		"grpc.meta.method_scope":     i.methodScope,
		"remote_ip":                  i.remoteIP,
		"user_id":                    i.userID,
		"username":                   i.userName,
		"correlation_id":             i.correlationID,
	} {
		if value == "" || value == unknownValue {
			continue
		}

		tags.Set(key, value)
	}
}

func (i requestInfo) reportPrometheusMetrics(err error) {
	grpcCode := structerr.GRPCCode(err)
	serviceName, methodName := extractServiceAndMethodName(i.fullMethod)

	requests.WithLabelValues(
		i.clientName,      // client_name
		serviceName,       // grpc_service
		methodName,        // grpc_method
		i.callSite,        // call_site
		i.authVersion,     // auth_version
		grpcCode.String(), // grpc_code
		i.deadlineType,    // deadline_type
		i.methodOperation,
		i.methodScope,
	).Inc()
	grpcprometheus.WithConstLabels(prometheus.Labels{"deadline_type": i.deadlineType})
}

func extractServiceAndMethodName(fullMethodName string) (string, string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/") // remove leading slash
	service, method, ok := strings.Cut(fullMethodName, "/")
	if !ok {
		return unknownValue, unknownValue
	}
	return service, method
}

// UnaryInterceptor returns a Unary Interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, serverInfo *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	info := newRequestInfo(ctx, serverInfo.FullMethod, "unary")

	info.injectTags(ctx)
	res, err := handler(ctx, req)
	info.reportPrometheusMetrics(err)

	return res, err
}

// StreamInterceptor returns a Stream Interceptor
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, serverInfo *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	info := newRequestInfo(ctx, serverInfo.FullMethod, streamRPCType(serverInfo))

	info.injectTags(ctx)
	err := handler(srv, stream)
	info.reportPrometheusMetrics(err)

	return err
}

func streamRPCType(info *grpc.StreamServerInfo) string {
	if info.IsClientStream && !info.IsServerStream {
		return "client_stream"
	} else if !info.IsClientStream && info.IsServerStream {
		return "server_stream"
	}
	return "bidi_stream"
}
