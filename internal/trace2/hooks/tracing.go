package hooks

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/trace2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/tracing"
)

type TracingHook struct{}

func (t TracingHook) Activate(context.Context, string, []string) (trace2.TraceHandler, bool) {
	return func(rootCtx context.Context, trace *trace2.Trace) {
		trace.Walk(rootCtx, func(ctx context.Context, trace *trace2.Trace) context.Context {
			if trace.IsRoot() {
				return rootCtx
			}

			spanName := fmt.Sprintf("git:%s", trace.Name)
			span, parentCtx := tracing.StartSpanIfHasParent(ctx, spanName, nil, opentracing.StartTime(trace.StartTime))
			span.SetTag("thread", trace.Thread)
			span.SetTag("childID", trace.ChildID)
			for key, value := range trace.Metadata {
				span.SetTag(key, value)
			}
			span.FinishWithOptions(opentracing.FinishOptions{FinishTime: trace.FinishTime})

			return parentCtx
		})
	}, true
}
