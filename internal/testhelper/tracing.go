package testhelper

import (
	"fmt"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
)

type stubTracingReporterConfig struct {
	sampler jaeger.Sampler
}

// StubTracingReporterOption is a function that modifies the config of stubbed tracing reporter
type StubTracingReporterOption func(*stubTracingReporterConfig)

// NeverSampled is an option that makes the stubbed tracer never sample spans
func NeverSampled() StubTracingReporterOption {
	return func(conf *stubTracingReporterConfig) {
		conf.sampler = jaeger.NewConstSampler(false)
	}
}

// StubTracingReporter stubs the distributed tracing's global tracer. It returns a reporter that
// records all generated spans along the way. The data is cleaned up afterward after the test is
// done. As there is only one global tracer, this stub is not safe to run in parallel.
func StubTracingReporter(t *testing.T, opts ...StubTracingReporterOption) (*jaeger.InMemoryReporter, func()) {
	conf := &stubTracingReporterConfig{
		jaeger.NewConstSampler(true),
	}
	for _, opt := range opts {
		opt(conf)
	}

	reporter := jaeger.NewInMemoryReporter()
	tracer, tracerCloser := jaeger.NewTracer("", conf.sampler, reporter)

	old := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)

	return reporter, func() {
		MustClose(t, tracerCloser)
		opentracing.SetGlobalTracer(old)
	}
}

// Span is a struct that provides a more test-friendly way to assert distributed tracing spans.
type Span struct {
	// Operation is the name of the operation being traced.
	Operation string
	// StartTime is the point in time when the span started.
	StartTime time.Time
	// Duration captures the elapsed time of the operation traced by the span.
	Duration time.Duration
	// Tags is a map that contains key-value pairs of stringified tags associated with the span.
	Tags map[string]string
}

// ReportedSpans function converts the spans that were captured by the stubbed reporter into an
// assertable data structure. Initially, the collected traces are represented using opentracing's
// Span interface. However, this interface is not suitable for testing purposes. Therefore, we must
// cast them to a Jaeger Span, which contains a lot of information that is not relevant to testing.
// The new span struct that is created contains only essential and safe-for-testing fields
func ReportedSpans(t *testing.T, reporter *jaeger.InMemoryReporter) []*Span {
	var reportedSpans []*Span
	for _, span := range reporter.GetSpans() {
		jaegerSpan, ok := span.(*jaeger.Span)
		require.Truef(t, ok, "stubbed span must be a Jaeger span")

		reportedSpan := &Span{
			Operation: jaegerSpan.OperationName(),
			StartTime: jaegerSpan.StartTime(),
			Duration:  jaegerSpan.Duration(),
			Tags:      map[string]string{},
		}

		for key, value := range jaegerSpan.Tags() {
			reportedSpan.Tags[key] = fmt.Sprintf("%s", value)
		}
		reportedSpans = append(reportedSpans, reportedSpan)
	}
	return reportedSpans
}
