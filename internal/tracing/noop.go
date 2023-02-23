package tracing

import (
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

// NoopSpan is a dummy span implementing opentracing.Span interface. All data setting functions do
// nothing. Data getting functions return other dummy objects. Spans of this kind are not recorded
// later.
type NoopSpan struct{}

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) Finish() {}

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) FinishWithOptions(_ opentracing.FinishOptions) {}

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) Context() opentracing.SpanContext { return NoopSpanContext{} }

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) LogFields(...log.Field) {}

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) SetOperationName(string) opentracing.Span { return s }

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) Log(opentracing.LogData) {}

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) SetTag(string, interface{}) opentracing.Span { return s }

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) LogKV(...interface{}) {}

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) SetBaggageItem(string, string) opentracing.Span { return s }

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) BaggageItem(string) string { return "" }

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) Tracer() opentracing.Tracer { return &NoopTracer{} }

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) LogEvent(string) {}

//nolint:revive // This is unintentionally missing documentation.
func (s *NoopSpan) LogEventWithPayload(string, interface{}) {}

// NoopSpanContext is a dummy context returned by NoopSpan
type NoopSpanContext struct{}

//nolint:revive // This is unintentionally missing documentation.
func (n NoopSpanContext) ForeachBaggageItem(func(k string, v string) bool) {}

// NoopTracer is a dummy tracer returned by NoopSpan
type NoopTracer struct{}

//nolint:revive // This is unintentionally missing documentation.
func (n NoopTracer) StartSpan(string, ...opentracing.StartSpanOption) opentracing.Span {
	return &NoopSpan{}
}

//nolint:revive // This is unintentionally missing documentation.
func (n NoopTracer) Inject(opentracing.SpanContext, interface{}, interface{}) error {
	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (n NoopTracer) Extract(interface{}, interface{}) (opentracing.SpanContext, error) {
	return &NoopSpanContext{}, nil
}
