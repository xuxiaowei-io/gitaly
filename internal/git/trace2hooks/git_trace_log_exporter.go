package trace2hooks

import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"golang.org/x/time/rate"
)

const (
	// maxEventPerSecond is the maximum number of events that can be processed per second
	maxEventPerSecond = 25
	// Rate limiter is immediately allocated the maxBurstToken value. Burst is the maximum number of tokens
	// that can be consumed in a single call
	maxBurstToken = 25
)

// NewGitTraceLogExporter initializes GitTraceLogExporter, which is a hook that uses the parsed
// trace2 events from the manager to export them to the Gitaly log. It's invocations are limited
// by the rateLimiter. The limiter allows maxBurstToken number of events to happen at once and then
// replenishes by maxEventPerSecond. It works on the token bucket algorithm where you have a number
// of tokens in the bucket to start and you can consume them in each call whilst the bucket gets
// refilled at the specified rate.
func NewGitTraceLogExporter() *GitTraceLogExporter {
	return &GitTraceLogExporter{
		rateLimiter: rate.NewLimiter(maxEventPerSecond, maxBurstToken),
	}
}

// GitTraceLogExporter is a trace2 hook that adds trace2 api event logs to Gitaly's logs.
type GitTraceLogExporter struct {
	rateLimiter *rate.Limiter
}

// Name returns the name of tracing exporter
func (t *GitTraceLogExporter) Name() string {
	return "git_trace_log_exporter"
}

// Handle will log the trace in a readable json format in Gitaly's logs. Metadata is also collected
// and additional information is added to the log. It is also rate limited to protect it from overload
// when there are a lot of trace2 events triggered from git operations.
func (t *GitTraceLogExporter) Handle(rootCtx context.Context, trace *trace2.Trace, logger log.Logger) error {
	if trace != nil {
		if !t.rateLimiter.Allow() {
			// when the event is not allowed, return an error to the caller, this may cause traces to be skipped/dropped
			return fmt.Errorf("rate has exceeded current limit")
		}

		trace.Walk(rootCtx, func(ctx context.Context, t *trace2.Trace) context.Context {
			if t.Metadata == nil {
				t.Metadata = make(map[string]string)
			}
			t.Metadata["elapsed_ms"] = fmt.Sprintf("%d", t.FinishTime.Sub(t.StartTime).Milliseconds())

			return ctx
		})

		childrenJSON, err := json.Marshal(trace.Children)
		if err != nil {
			return fmt.Errorf("marshalling error in hook %s: %s", "trace2hooks."+t.Name(), err.Error())
		}
		escapedChildren := json.RawMessage(childrenJSON)

		logger.WithFields(log.Fields{
			"name":       trace.Name,
			"thread":     trace.Thread,
			"component":  "trace2hooks." + t.Name(),
			"start_time": trace.StartTime,
			"end_time":   trace.FinishTime,
			"metadata":   trace.Metadata,
			"children":   escapedChildren,
		}).Debug("Git Trace2 API")
	}
	return nil
}
