package limithandler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backoff"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// GrpcPushbackHeader is the key for gRPC response header that defines the duration in
	// milliseconds a client should back-off before re-send the request again. This header is
	// only effective if there is a retry policy configuration for the RPC. The retry policy
	// must declare MaxAttempts and RetryableStatusCodes. This pushback duration has higher
	// precedence than retry settings in the retry policy.
	//
	// When a client receives this header, it is forced to sleep. As this feature is supported
	// in gRPC library implementations, clients can't refuse. It's recommended to add this
	// header to protect most critical, resource-hungry RPCs, such as UploadPack, PackObject.
	// Please be nice!
	//
	// For more information, visit https://github.com/grpc/proposal/blob/master/A6-client-retries.md#pushback
	GrpcPushbackHeader = "grpc-retry-pushback-ms"
	// GrpcPreviousAttempts defines the key of gRPC request header that stores the amount of
	// previous attempts of the same RPC. This number counts "transparent" failures and failures
	// matching RetryableStatusCodes retry config. This counter stays in the library layer. If
	// the application layer performs the retry, this value is always set to 0.
	GrpcPreviousAttempts = "grpc-previous-rpc-attempts"
)

// grpcPushbackMaxAttempt defines the maximum attempt a client should retry. Other type of
// transient failures, such as network failures, are also taken into account.
var grpcPushbackMaxAttempt = 3

// grpcPushbackRetryableStatusCodes defines the list of gRPC codes a client should perform retry
// automatically. The status codes are capitalized SNAKE_CASE. The following link contains the list
// of all codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
var grpcPushbackRetryableStatusCodes = []string{"RESOURCE_EXHAUSTED"}

// Exponential backoff parameters
var (
	initialBackoff    = 5 * time.Second
	maxBackoff        = 60 * time.Second
	backoffMultiplier = 2.0
)

// RPCsWithPushbackHeaders defines the list of Gitaly RPCs to add pushback support. Please note
// that the list must only include external RPCs from clients. Adding an internal RPC, such as
// /gitaly.HookService/PackObjectsHookWithSidechannel, makes Gitaly pushes itself.
var RPCsWithPushbackHeaders = []string{
	"/gitaly.SSHService/SSHUploadPackWithSidechannel",
	"/gitaly.SmartHTTPService/PostUploadPackWithSidechannel",
}

func init() {
	for _, rpc := range RPCsWithPushbackHeaders {
		if _, err := protoregistry.GitalyProtoPreregistered.LookupMethod(rpc); err != nil {
			panic(fmt.Errorf("RPC not found: %s", rpc))
		}
	}
}

// DefaultPushbackMethodConfigs returns the list of gRPC method configs. Each method config includes
// name of RPC with pushback enabled, maximum attempts, and retryable status codes. This list should
// be added to client Service Config when it dials.
func DefaultPushbackMethodConfigs() []*gitalypb.MethodConfig {
	var configs []*gitalypb.MethodConfig

	for _, rpc := range RPCsWithPushbackHeaders {
		// Method is validated when this package is loaded
		mth, _ := protoregistry.GitalyProtoPreregistered.LookupMethod(rpc)

		serviceName, methodName := mth.ServiceNameAndMethodName()
		configs = append(configs, &gitalypb.MethodConfig{
			Name: []*gitalypb.MethodConfig_Name{{
				Service: serviceName,
				Method:  methodName,
			}},
			// When specify pushback header grpc-retry-pushback-ms, client uses that
			// value. Other exponential backoff parameters in RetryPolicy are ignored.
			// We supply them here to make valid
			RetryPolicy: &gitalypb.MethodConfig_RetryPolicy{
				MaxAttempts:          uint32(grpcPushbackMaxAttempt),
				RetryableStatusCodes: grpcPushbackRetryableStatusCodes,
				InitialBackoff:       durationpb.New(initialBackoff),
				MaxBackoff:           durationpb.New(maxBackoff),
				BackoffMultiplier:    float32(backoffMultiplier),
			},
		})
	}
	return configs
}

// newLimitErrorBackoff returns an exponential backoff strategy when facing limit error. The backoff
// parameters are adjusted much longer and steeper than normal networking failure. When Gitaly node
// is saturated and starts to push-back traffic, it takes a lot of time for the situation to go
// away. The node can either wait for more room to breath or terminate in-flight requests. Either
// way, it does not make sense for clients to retry in short delays.
//
// | Retries | Delay before random jitter |
// | ------- | -------------------------- |
// | 0       | 5 second                   |
// | 1       | 10 seconds                 |
// | 2       | 20 seconds                 |
// | 3       | 40 seconds                 |
func newLimitErrorBackoff() backoff.Strategy {
	exponential := backoff.NewDefaultExponential(rand.New(rand.NewSource(time.Now().UnixNano())))
	exponential.BaseDelay = initialBackoff
	exponential.MaxDelay = maxBackoff
	exponential.Multiplier = backoffMultiplier
	return exponential
}

func defaultPushbackPolicies() map[string]backoff.Strategy {
	policies := map[string]backoff.Strategy{}
	for _, rpc := range RPCsWithPushbackHeaders {
		policies[rpc] = newLimitErrorBackoff()
	}
	return policies
}

type pushback struct {
	policies map[string]backoff.Strategy
}

func (p *pushback) push(ctx context.Context, fullMethod string, err error) {
	if !errors.Is(err, ErrMaxQueueSize) && !errors.Is(err, ErrMaxQueueTime) {
		return
	}
	var strategy backoff.Strategy
	strategy, exist := p.policies[fullMethod]
	if !exist {
		return
	}

	var attempts uint
	if strAttempts := metadata.ValueFromIncomingContext(ctx, GrpcPreviousAttempts); len(strAttempts) > 0 {
		parsedAttempts, err := strconv.ParseInt(strAttempts[0], 10, 32)
		if err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Error("fail to parse gRPC previous retry attempts")
			return
		}
		attempts = uint(parsedAttempts)
	}

	pushbackDuration := strategy.Backoff(attempts)
	p.setResponseHeader(ctx, pushbackDuration)
	p.setErrorDetail(err, pushbackDuration)
}

func (p *pushback) setResponseHeader(ctx context.Context, pushbackDuration time.Duration) {
	if err := grpc.SetTrailer(ctx, metadata.MD{GrpcPushbackHeader: []string{fmt.Sprintf("%d", pushbackDuration.Milliseconds())}}); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Error("fail to set gRPC push-back header")
	}
}

func (p *pushback) setErrorDetail(err error, pushbackDuration time.Duration) {
	var structErr structerr.Error
	if errors.As(err, &structErr) {
		for _, detail := range structErr.Details() {
			if limitError, ok := detail.(*gitalypb.LimitError); ok {
				// The underlying layers can specify its own RetryAfter value. The
				// middleware should honor that decision.
				if limitError.RetryAfter.AsDuration() == time.Duration(0) {
					limitError.RetryAfter = durationpb.New(pushbackDuration)
				}
			}
		}
	}
}
