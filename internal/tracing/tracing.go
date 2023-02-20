package tracing

import (
	"context"

	"github.com/opentracing/opentracing-go"
)

// Tags is a key-value map. It is used to set tags for a span
type Tags map[string]any

// StartSpan creates a new span with name and options (mostly tags). This function is a wrapper for
// underlying tracing libraries. This method should only be used at the entrypoint of the program.
func StartSpan(ctx context.Context, spanName string, tags Tags) (opentracing.Span, context.Context) {
	return opentracing.StartSpanFromContext(ctx, spanName, tagsToOpentracingTags(tags)...)
}

// StartSpanIfHasParent creates a new span if the context already has an existing span. This function
// adds a simple validation to prevent orphan spans outside interested code paths. It returns a dummy
// span, which acts as normal span, but does absolutely nothing and is not recorded later.
func StartSpanIfHasParent(ctx context.Context, spanName string, tags Tags) (opentracing.Span, context.Context) {
	parent := opentracing.SpanFromContext(ctx)
	if parent == nil {
		return &NoopSpan{}, ctx
	}
	return opentracing.StartSpanFromContext(ctx, spanName, tagsToOpentracingTags(tags)...)
}

// DiscardSpanInContext discards the current active span from the context. This function is helpful
// when the current code path enters an area shared by other code paths. Git catfile cache is a
// good example of this type of span.
func DiscardSpanInContext(ctx context.Context) context.Context {
	if opentracing.SpanFromContext(ctx) == nil {
		return ctx
	}
	return opentracing.ContextWithSpan(ctx, nil)
}

func tagsToOpentracingTags(tags Tags) []opentracing.StartSpanOption {
	var opts []opentracing.StartSpanOption
	for key, value := range tags {
		opts = append(opts, opentracing.Tag{Key: key, Value: value})
	}
	return opts
}
