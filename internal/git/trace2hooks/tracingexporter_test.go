package trace2hooks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/tracing"
)

func TestTracingExporter_Handle(t *testing.T) {
	reporter, cleanup := testhelper.StubTracingReporter(t)
	defer cleanup()

	// Pin a timestamp for trace tree generation below. This way enables asserting the time
	// frames of spans correctly.
	current, err := time.Parse("2006-01-02T15:04:05Z", "2023-01-01T00:00:00Z")
	require.NoError(t, err)

	//                         0s    1s    2s    3s    4s    5s    6s    7s
	// root                    |---->|---->|---->|---->|---->|---->|---->|
	// version                 |---->|     |     |     |     |     |     |
	// start                   |     |---->|     |     |     |     |     |
	// def_repo                |     |     |---->|     |     |     |     |
	// index:do_read_index     |     |     |     |---->|---->|---->|     |
	// cache_tree:read         |     |     |     |---->|     |     |     |
	// data:index:read/version |     |     |     |     |---->|     |     |
	// data:index:read/cache_nr|     |     |     |     |     |---->|     |
	// submodule:parallel/fetch|     |     |     |     |     |     |---->|
	exampleTrace := connectChildren(&trace2.Trace{
		Thread:     "main",
		Name:       "root",
		StartTime:  current,
		FinishTime: current.Add(7 * time.Second),
		Children: []*trace2.Trace{
			{
				Thread:     "main",
				Name:       "version",
				StartTime:  current,
				FinishTime: current.Add(1 * time.Second),
			},
			{
				Thread:     "main",
				Name:       "start",
				StartTime:  current.Add(1 * time.Second),
				FinishTime: current.Add(2 * time.Second),
				Metadata:   map[string]string{"argv": "git fetch origin master"},
			},
			{
				Thread:     "main",
				Name:       "def_repo",
				StartTime:  current.Add(2 * time.Second),
				FinishTime: current.Add(3 * time.Second),
			},
			connectChildren(&trace2.Trace{
				Thread:     "main",
				Name:       "index:do_read_index",
				StartTime:  current.Add(3 * time.Second),
				FinishTime: current.Add(6 * time.Second),
				Children: []*trace2.Trace{
					{
						Thread:     "main",
						Name:       "cache_tree:read",
						ChildID:    "0",
						StartTime:  current.Add(3 * time.Second),
						FinishTime: current.Add(4 * time.Second),
					},
					{
						Thread:     "main",
						ChildID:    "0",
						Name:       "data:index:read/version",
						StartTime:  current.Add(4 * time.Second),
						FinishTime: current.Add(5 * time.Second),
						Metadata:   map[string]string{"data": "2"},
					},
					{
						Thread:     "main",
						ChildID:    "0",
						Name:       "data:index:read/cache_nr",
						StartTime:  current.Add(5 * time.Second),
						FinishTime: current.Add(6 * time.Second),
						Metadata:   map[string]string{"data": "1500"},
					},
				},
			}),
			{
				Thread:     "main",
				Name:       "submodule:parallel/fetch",
				StartTime:  current.Add(6 * time.Second),
				FinishTime: current.Add(7 * time.Second),
			},
		},
	})

	for _, tc := range []struct {
		desc          string
		setup         func(*testing.T) (context.Context, *trace2.Trace)
		expectedSpans []*testhelper.Span
	}{
		{
			desc: "empty trace",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				_, ctx := tracing.StartSpan(testhelper.Context(t), "root", nil)
				return ctx, nil
			},
			expectedSpans: nil,
		},
		{
			desc: "receives trace consisting of root only",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				_, ctx := tracing.StartSpan(testhelper.Context(t), "root", nil)
				return ctx, &trace2.Trace{
					Thread:     "main",
					Name:       "root",
					StartTime:  current,
					FinishTime: time.Time{},
				}
			},
			expectedSpans: nil,
		},
		{
			desc: "receives a complete trace",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				_, ctx := tracing.StartSpan(testhelper.Context(t), "root", nil)

				return ctx, exampleTrace
			},
			expectedSpans: []*testhelper.Span{
				{
					Operation: "git:version",
					StartTime: current,
					Duration:  1 * time.Second,
					Tags: map[string]string{
						"childID": "",
						"thread":  "main",
					},
				},
				{
					Operation: "git:start",
					StartTime: current.Add(1 * time.Second),
					Duration:  1 * time.Second,
					Tags: map[string]string{
						"argv":    "git fetch origin master",
						"childID": "",
						"thread":  "main",
					},
				},
				{
					Operation: "git:def_repo",
					StartTime: current.Add(2 * time.Second),
					Duration:  1 * time.Second,
					Tags: map[string]string{
						"childID": "",
						"thread":  "main",
					},
				},
				{
					Operation: "git:index:do_read_index",
					StartTime: current.Add(3 * time.Second),
					Duration:  3 * time.Second, // 3 children
					Tags: map[string]string{
						"childID": "",
						"thread":  "main",
					},
				},
				{
					Operation: "git:cache_tree:read",
					StartTime: current.Add(3 * time.Second),
					Duration:  1 * time.Second, // 3 children
					Tags: map[string]string{
						"childID": "0",
						"thread":  "main",
					},
				},
				{
					Operation: "git:data:index:read/version",
					StartTime: current.Add(4 * time.Second),
					Duration:  1 * time.Second, // 3 children
					Tags: map[string]string{
						"childID": "0",
						"thread":  "main",
						"data":    "2",
					},
				},
				{
					Operation: "git:data:index:read/cache_nr",
					StartTime: current.Add(5 * time.Second),
					Duration:  1 * time.Second, // 3 children
					Tags: map[string]string{
						"childID": "0",
						"thread":  "main",
						"data":    "1500",
					},
				},
				{
					Operation: "git:submodule:parallel/fetch",
					StartTime: current.Add(6 * time.Second),
					Duration:  1 * time.Second, // 3 children
					Tags: map[string]string{
						"childID": "",
						"thread":  "main",
					},
				},
			},
		},
		{
			desc: "receives a complete trace but tracing is not initialized",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				return testhelper.Context(t), exampleTrace
			},
			expectedSpans: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			reporter.Reset()
			ctx, trace := tc.setup(t)

			exporter := TracingExporter{}
			err := exporter.Handle(ctx, trace)
			require.NoError(t, err)

			spans := testhelper.ReportedSpans(t, reporter)
			require.Equal(t, tc.expectedSpans, spans)
		})
	}
}

func connectChildren(trace *trace2.Trace) *trace2.Trace {
	for _, t := range trace.Children {
		t.Parent = trace
	}
	return trace
}
