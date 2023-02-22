package trace2

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Trace denotes a node in the tree representation of Git Trace2 events. A node is not necessary
// a one-one mapping of an event.
type Trace struct {
	// Thread is the name of the thread of the corresponding event. The default thread name is
	// "main". A new thread is assigned with a new name.
	Thread string
	// Name denotes the name of the trace. The node name depends on the event types. Data-type
	// trace name is the most significant. It can be used to access the accurate data trace node
	// For example: data:index:refresh/sum_scan
	Name string
	// StartTime is the starting time of the trace
	StartTime time.Time
	// FinishTime is the starting time of the trace
	FinishTime time.Time
	// Metadata is a map of metadata and data extracted from the event. A data-type trace always
	// stores its data under "data" key of this map
	Metadata map[string]string
	// ChildID is the unique ID assigned by the parent process when it spawns a sub-process
	// The ID of root process is empty.
	ChildID string
	// Parent points to the parent node of the current trace. The root node's parent is nil
	Parent *Trace
	// Children stores the list of order-significant traces belong to the current trace
	Children []*Trace
	// Depth indicates the depth of the trace node
	Depth int
}

// IsRoot returns true if the current trace is the root of the tree
func (trace *Trace) IsRoot() bool {
	return trace.Parent == nil
}

// Walk performs in-order tree traversal. It stops at each node and trigger handler function with
// the current trace.
func (trace *Trace) Walk(ctx context.Context, handler func(context.Context, *Trace) context.Context) {
	if trace == nil {
		return
	}
	ctx = handler(ctx, trace)
	for _, child := range trace.Children {
		child.Walk(ctx, handler)
	}
}

// Inspect returns the formatted string of the tree. It mimics the format for trace2's performance
// target: https://git-scm.com/docs/api-trace2#_perf_format. It's mostly used for testing and
// debugging purpose.
func (trace *Trace) Inspect() string {
	var output string
	trace.Walk(context.Background(), func(ctx context.Context, t *Trace) context.Context {
		line := fmt.Sprintf(
			"%s | %s | %s | %s%s %s",
			t.StartTime.Format("03:04:05.000000"),
			t.FinishTime.Format("03:04:05.000000"),
			t.fullThread(),
			strings.Repeat(".", t.Depth),
			t.Name,
			t.inspectMetadata(),
		)
		if output != "" {
			output += "\n"
		}
		output += strings.TrimSpace(line)
		return ctx
	})
	return output
}

func (trace *Trace) setName(hints []string) {
	var parts []string
	for _, s := range hints {
		if strings.TrimSpace(s) != "" {
			parts = append(parts, s)
		}
	}
	trace.Name = strings.Join(parts, ":")
}

func (trace *Trace) setMetadata(key, value string) {
	if trace.Metadata == nil {
		trace.Metadata = map[string]string{}
	}
	trace.Metadata[key] = value
}

func (trace *Trace) fullThread() string {
	if trace.ChildID != "" {
		return fmt.Sprintf("%s (child %s)", trace.Thread, trace.ChildID)
	}
	return trace.Thread
}

func (trace *Trace) inspectMetadata() string {
	var metadata string
	if len(trace.Metadata) > 0 {
		var keys []string
		for key := range trace.Metadata {
			keys = append(keys, key)
		}
		// Sort metadata by key to make output deterministic
		sort.Strings(keys)
		for _, key := range keys {
			metadata += fmt.Sprintf("%s=%q ", key, trace.Metadata[key])
		}
		metadata = "(" + strings.TrimSpace(metadata) + ")"
	}
	return metadata
}
