package serviceconfig

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Generate returns the recommended service config. This service config defines the default
// client-side load-balancing strategy (round-robin at this point). When autoRetry option is turned
// on, this method attaches the retry policy for all read-only RPCs. The list of services and
// methods are fetched from input registry.
func Generate(registry *protoregistry.Registry, autoRetry bool) *gitalypb.ServiceConfig {
	serviceConfig := &gitalypb.ServiceConfig{
		LoadBalancingConfig: []*gitalypb.LoadBalancingConfig{{
			Policy: &gitalypb.LoadBalancingConfig_RoundRobin{},
		}},
	}
	if autoRetry {
		serviceConfig.MethodConfig = generateAutoRetryMethodConfigs(registry)
	}
	return serviceConfig
}

func generateAutoRetryMethodConfigs(registry *protoregistry.Registry) []*gitalypb.MethodConfig {
	methods := getAllMethods(registry)

	// Generate method configs sorted by service. The sorting here is to make the resulting
	// policy's order stable. It's essential for debugging and tests. There is no difference
	// when it is parsed by gRPC
	services := make([]string, 0, len(methods))
	for service := range methods {
		services = append(services, service)
	}
	sort.Strings(services)

	methodConfigs := make([]*gitalypb.MethodConfig, 0, len(methods))
	for _, service := range services {
		methodConfigs = append(methodConfigs, generateMethodConfig(service, methods[service]))
	}
	return methodConfigs
}

func generateMethodConfig(service string, methods []string) *gitalypb.MethodConfig {
	methodConfig := &gitalypb.MethodConfig{
		Name: make([]*gitalypb.MethodConfig_Name, 0, len(methods)),
		RetryPolicy: &gitalypb.MethodConfig_RetryPolicy{
			MaxAttempts:          3,
			InitialBackoff:       durationpb.New(100 * time.Millisecond),
			MaxBackoff:           durationpb.New(time.Second),
			BackoffMultiplier:    2,
			RetryableStatusCodes: []string{"UNAVAILABLE", "DEADLINE_EXCEEDED"},
		},
	}
	sort.Strings(methods)
	for _, method := range methods {
		methodConfig.Name = append(methodConfig.Name, &gitalypb.MethodConfig_Name{
			Service: service,
			Method:  method,
		})
	}
	return methodConfig
}

func getAllMethods(registry *protoregistry.Registry) map[string][]string {
	services := make(map[string][]string)

	for _, method := range registry.Methods() {
		// Read-only requests are eligible for auto-retry. We use operation name to
		// differentiate read/write/maintenance RPCs. The maintenance RPCs are also unsafe
		// to retry.
		if method.Operation != protoregistry.OpAccessor {
			continue
		}

		nameSplit := strings.Split(strings.TrimPrefix(method.FullMethodName(), "/"), "/")
		if len(nameSplit) != 2 {
			// This branch will never happen. We generate fullMethodName is generated
			// from service and method name following this format.
			panic(fmt.Sprintf("RPC's full method name is malformed: %s", method.FullMethodName()))
		}

		service, methodName := nameSplit[0], nameSplit[1]
		if _, ok := services[service]; !ok {
			services[service] = make([]string, 0)
		}
		services[service] = append(services[service], methodName)
	}
	return services
}
