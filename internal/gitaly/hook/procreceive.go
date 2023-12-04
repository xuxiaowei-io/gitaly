package hook

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// ProcReceiveHook is used to intercept git-receive-pack(1)'s execute-commands code.
// This allows us to intercept the reference updates and avoid writing directly to
// the disk. The intercepted updates are then bundled into `procReceiveHookInvocation`
// and added to the registry. The RPC which invoked git-receive-pack(1) in the first
// place picks up the invocation from the RPC and accepts/rejects individual references.
func (m *GitLabHookManager) ProcReceiveHook(ctx context.Context, repo *gitalypb.Repository, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	payload, err := git.HooksPayloadFromEnv(env)
	if err != nil {
		return fmt.Errorf("extracting hooks payload: %w", err)
	}

	// This hook only works when there is a transaction present.
	if payload.TransactionID == 0 {
		return fmt.Errorf("no transaction found in payload")
	}

	scanner := pktline.NewScanner(stdin)

	// Version and feature negotiation.
	if !scanner.Scan() {
		return fmt.Errorf("expected input: %w", scanner.Err())
	}

	data, err := pktline.Payload(scanner.Bytes())
	if err != nil {
		return fmt.Errorf("receiving header: %w", err)
	}

	var featureRequests *procReceiveFeatureRequests
	after, ok := bytes.CutPrefix(data, []byte("version=1\000"))
	if !ok {
		return fmt.Errorf("unsupported version: %s", data)
	}

	featureRequests, err = parseFeatureRequest(after)
	if err != nil {
		return fmt.Errorf("parsing feature request: %w", err)
	}

	if !scanner.Scan() {
		return fmt.Errorf("expected input: %w", scanner.Err())
	}

	if !pktline.IsFlush(scanner.Bytes()) {
		return fmt.Errorf("expected pkt flush")
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("parsing stdin: %w", err)
	}

	if _, err := pktline.WriteString(stdout, fmt.Sprintf("version=1\000%s", featureRequests)); err != nil {
		return fmt.Errorf("writing version: %w", err)
	}

	if err := pktline.WriteFlush(stdout); err != nil {
		return fmt.Errorf("flushing version: %w", err)
	}

	updates := []ReferenceUpdate{}
	for scanner.Scan() {
		bytes := scanner.Bytes()

		// When all reference updates are transmitted, we expect a flush.
		if pktline.IsFlush(bytes) {
			break
		}

		data, err := pktline.Payload(bytes)
		if err != nil {
			return fmt.Errorf("receiving reference update: %w", err)
		}

		update, err := parseRefUpdate(data)
		if err != nil {
			return fmt.Errorf("parse reference update: %w", err)
		}
		updates = append(updates, update)
	}

	invocation := newProcReceiveHookInvocation(
		featureRequests.atomic,
		payload.TransactionID,
		updates,
		func(referenceName git.ReferenceName) error {
			if _, err := pktline.WriteString(stdout, fmt.Sprintf("ok %s", referenceName)); err != nil {
				return fmt.Errorf("write ref %s ok: %w", referenceName, err)
			}

			return nil
		},
		func(referenceName git.ReferenceName, reason string) error {
			if _, err := pktline.WriteString(stdout, fmt.Sprintf("ng %s %s", referenceName, reason)); err != nil {
				return fmt.Errorf("write ref %s ng: %w", referenceName, err)
			}

			return nil
		},
		func() error {
			if err := pktline.WriteFlush(stdout); err != nil {
				return fmt.Errorf("flushing updates: %w", err)
			}

			return nil
		})

	m.procReceiveRegistry.set(invocation)

	return nil
}

func parseRefUpdate(data []byte) (ReferenceUpdate, error) {
	var update ReferenceUpdate

	split := bytes.Split(data, []byte(" "))
	if len(split) != 3 {
		return update, fmt.Errorf("unknown ref update format: %s", split)
	}

	update.Ref = git.ReferenceName(split[2])
	update.OldOID = git.ObjectID(split[0])
	update.NewOID = git.ObjectID(split[1])

	return update, nil
}

type procReceiveFeatureRequests struct {
	atomic bool
}

func (r *procReceiveFeatureRequests) String() string {
	s := ""
	if r.atomic {
		s = "atomic"
	}

	return s
}

// parseFeatureRequest parses the features requested.
func parseFeatureRequest(data []byte) (*procReceiveFeatureRequests, error) {
	var featureRequests procReceiveFeatureRequests

	for _, feature := range bytes.Split(data, []byte(" ")) {
		if bytes.Equal(feature, []byte("atomic")) {
			featureRequests.atomic = true
		}
	}

	return &featureRequests, nil
}
