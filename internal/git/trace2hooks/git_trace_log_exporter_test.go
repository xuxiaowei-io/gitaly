package trace2hooks

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"golang.org/x/time/rate"
)

func TestGitTraceLogExporter_Handle(t *testing.T) {
	current, _ := time.Parse("2006-01-02T15:04:05Z", "2023-01-01T00:00:00Z")
	endTime := current.Add(7 * time.Second)

	//                           0s    1s    2s    3s    4s    5s    6s    7s
	// root                      |---->|---->|---->|---->|---->|---->|---->|
	// version                   |---->|     |     |     |     |     |     |
	// start                     |     |---->|     |     |     |     |     |
	// def_repo                  |     |     |---->|     |     |     |     |
	// index:do_read_index       |     |     |     |---->|---->|---->|     |
	// cache_tree:read           |     |     |     |---->|     |     |     |
	// data:index:read/version   |     |     |     |     |---->|     |     |
	// data_json:bloom/statistics|     |     |     |     |     |---->|     |
	// submodule:parallel/fetch  |     |     |     |     |     |     |---->|
	exampleTrace := connectChildren(&trace2.Trace{
		Thread:     "main",
		Name:       "root",
		StartTime:  current,
		FinishTime: endTime,
		Depth:      0,
		Children: []*trace2.Trace{
			{
				Thread:     "main",
				Name:       "version",
				StartTime:  current,
				FinishTime: current.Add(1 * time.Second), Metadata: map[string]string{"exe": "2.42.0"},
				Depth: 1,
			},
			{
				Thread:     "main",
				Name:       "start",
				StartTime:  current.Add(1 * time.Second),
				FinishTime: current.Add(2 * time.Second),
				Metadata:   map[string]string{"argv": "git fetch origin master"},
				Depth:      1,
			},
			{
				Thread:     "main",
				Name:       "def_repo",
				StartTime:  current.Add(2 * time.Second),
				FinishTime: current.Add(3 * time.Second),
				Metadata:   map[string]string{"worktree": "/Users/userx123/Documents/gitlab-development-kit"},
				Depth:      1,
			},
			connectChildren(&trace2.Trace{
				Thread:     "main",
				Name:       "index:do_read_index",
				StartTime:  current.Add(3 * time.Second),
				FinishTime: current.Add(6 * time.Second),
				Depth:      1,
				Children: []*trace2.Trace{
					{
						Thread:     "main",
						Name:       "cache_tree:read",
						ChildID:    "0",
						StartTime:  current.Add(3 * time.Second),
						FinishTime: current.Add(4 * time.Second),
						Depth:      2,
					},
					{
						Thread:     "main",
						ChildID:    "0",
						Name:       "data:index:read/version",
						StartTime:  current.Add(4 * time.Second),
						FinishTime: current.Add(5 * time.Second),
						Metadata:   map[string]string{"data": "2"},
						Depth:      2,
					},
					{
						Thread:     "main",
						ChildID:    "0",
						Name:       "data_json:bloom/statistics",
						StartTime:  current.Add(5 * time.Second),
						FinishTime: current.Add(6 * time.Second),
						Metadata: map[string]string{
							"data": "{\"filter_not_present\":0,\"maybe\":1,\"definitely_not\":2,\"false_positive\":0}",
						},
						Depth: 2,
					},
				},
			}),
			{
				Thread:     "main",
				Name:       "submodule:parallel/fetch",
				StartTime:  current.Add(6 * time.Second),
				FinishTime: current.Add(7 * time.Second),
				Depth:      1,
			},
		},
	})

	for _, tc := range []struct {
		desc          string
		setup         func(*testing.T) (context.Context, *trace2.Trace)
		expectedTrace trace2.Trace
	}{
		{
			desc: "empty trace",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				ctx := testhelper.Context(t)
				return ctx, nil
			},
			expectedTrace: trace2.Trace{},
		},
		{
			desc: "receives trace consisting of root only",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				ctx := testhelper.Context(t)
				return ctx, &trace2.Trace{
					Thread:     "main",
					Name:       "root",
					StartTime:  current,
					FinishTime: endTime,
				}
			},
			expectedTrace: trace2.Trace{
				Thread:     "main",
				Name:       "root",
				StartTime:  current,
				FinishTime: endTime,
				Metadata:   map[string]string{"elapsed_ms": "7000"},
			},
		},
		{
			desc: "receives a complete trace",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				ctx := testhelper.Context(t)
				return ctx, exampleTrace
			},
			expectedTrace: trace2.Trace{
				Thread:     "main",
				Name:       "root",
				StartTime:  current,
				FinishTime: endTime,
				Metadata:   map[string]string{"elapsed_ms": "7000"},
				Children: []*trace2.Trace{
					{
						Thread:     "main",
						Name:       "version",
						StartTime:  current,
						FinishTime: current.Add(1 * time.Second), Metadata: map[string]string{"exe": "2.42.0", "elapsed_ms": "1000"},
						Depth: 1,
					}, {
						Thread:     "main",
						Name:       "start",
						StartTime:  current.Add(1 * time.Second),
						FinishTime: current.Add(2 * time.Second),
						Metadata:   map[string]string{"argv": "git fetch origin master", "elapsed_ms": "1000"},
						Depth:      1,
					}, {
						Thread:     "main",
						Name:       "def_repo",
						StartTime:  current.Add(2 * time.Second),
						FinishTime: current.Add(3 * time.Second),
						Metadata:   map[string]string{"worktree": "/Users/userx123/Documents/gitlab-development-kit", "elapsed_ms": "1000"},
						Depth:      1,
					}, {
						Thread:     "main",
						Name:       "index:do_read_index",
						StartTime:  current.Add(3 * time.Second),
						FinishTime: current.Add(6 * time.Second),
						Metadata:   map[string]string{"elapsed_ms": "3000"},
						Depth:      1,
						Children: []*trace2.Trace{
							{
								Thread:     "main",
								ChildID:    "0",
								Name:       "cache_tree:read",
								StartTime:  current.Add(3 * time.Second),
								FinishTime: current.Add(4 * time.Second),
								Metadata:   map[string]string{"elapsed_ms": "1000"},
								Depth:      2,
							},
							{
								Thread:     "main",
								ChildID:    "0",
								Name:       "data:index:read/version",
								StartTime:  current.Add(4 * time.Second),
								FinishTime: current.Add(5 * time.Second),
								Metadata:   map[string]string{"data": "2", "elapsed_ms": "1000"},
								Depth:      2,
							},
							{
								Thread:     "main",
								ChildID:    "0",
								Name:       "data_json:bloom/statistics",
								StartTime:  current.Add(5 * time.Second),
								FinishTime: current.Add(6 * time.Second),
								Metadata: map[string]string{
									"elapsed_ms": "1000",
									"data":       "{\"filter_not_present\":0,\"maybe\":1,\"definitely_not\":2,\"false_positive\":0}",
								},
								Depth: 2,
							},
						},
					}, {
						Thread:     "main",
						Name:       "submodule:parallel/fetch",
						StartTime:  current.Add(6 * time.Second),
						FinishTime: current.Add(7 * time.Second),
						Metadata:   map[string]string{"elapsed_ms": "1000"},
						Depth:      1,
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, trace := tc.setup(t)
			logger := testhelper.NewLogger(t)
			logger.LogrusEntry().Logger.SetLevel(logrus.DebugLevel) //nolint:staticcheck
			hook := testhelper.AddLoggerHook(logger)
			exporter := NewGitTraceLogExporter()
			// execute and assertions
			err := exporter.Handle(ctx, trace, logger)
			require.NoError(t, err)
			logEntry := hook.LastEntry()
			if trace != nil {
				assert.Equal(t, tc.expectedTrace.Thread, logEntry.Data["thread"])
				assert.Equal(t, tc.expectedTrace.Name, logEntry.Data["name"])
				assert.Equal(t, tc.expectedTrace.Metadata, logEntry.Data["metadata"])
				assert.Equal(t, tc.expectedTrace.StartTime, logEntry.Data["start_time"])
				assert.Equal(t, tc.expectedTrace.FinishTime, logEntry.Data["end_time"])
				var children []*trace2.Trace
				childErr := json.Unmarshal(logEntry.Data["children"].(json.RawMessage), &children)
				require.NoError(t, childErr)
				assert.Equal(t, tc.expectedTrace.Children, children)
			}
		})
	}
}

func TestGitTraceLogExporter_rateLimitFailureMock(t *testing.T) {
	errorMsg := "rate has exceeded current limit"

	for _, tc := range []struct {
		desc          string
		setup         func(*testing.T) (context.Context, *trace2.Trace)
		expectedError *regexp.Regexp
	}{
		{
			desc: "rate limit exceeded",
			setup: func(t *testing.T) (context.Context, *trace2.Trace) {
				ctx := testhelper.Context(t)
				return ctx, &trace2.Trace{
					Thread:     "main",
					Name:       "root",
					StartTime:  time.Time{},
					FinishTime: time.Time{}.Add(1 * time.Second),
				}
			},
			expectedError: regexp.MustCompile(errorMsg),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, trace := tc.setup(t)
			logger := testhelper.SharedLogger(t)

			rl := rate.NewLimiter(0, 1) // burst limit of 1, refreshing at 0 rps

			exporter := GitTraceLogExporter{rateLimiter: rl}
			err := exporter.Handle(ctx, trace, logger)
			require.NoError(t, err)
			err = exporter.Handle(ctx, trace, logger)
			require.Error(t, err)
			require.Regexp(t, tc.expectedError, err)
			if err != nil {
				fmt.Printf("%v Error : %v\n", time.Now().Format("15:04:05"), err)
				return
			}
		})
	}
}
