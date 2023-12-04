package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/storagemgr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestProcReceiveHook(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	locator := config.NewLocator(cfg)

	txManager := transaction.NewTrackingManager()

	receiveHooksPayload := &git.UserDetails{
		UserID:   "1234",
		Username: "user",
		Protocol: "web",
	}

	payload, err := git.NewHooksPayload(
		cfg,
		repo,
		gittest.DefaultObjectHash,
		nil,
		receiveHooksPayload,
		git.PreReceiveHook,
		featureflag.FromContext(ctx),
		1,
	).Env()
	require.NoError(t, err)

	procReceiveRegistry := NewProcReceiveRegistry()

	hookManager := NewManager(
		cfg,
		locator,
		testhelper.SharedLogger(t),
		gitCmdFactory,
		txManager,
		gitlab.NewMockClient(t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive),
		NewTransactionRegistry(storagemgr.NewTransactionRegistry()),
		procReceiveRegistry,
	)

	type setupData struct {
		env             []string
		ctx             context.Context
		stdin           string
		expectedErr     error
		expectedStdout  string
		expectedUpdates []ReferenceUpdate
		expectedAtomic  bool
		invocationSteps func(invocation ProcReceiveHookInvocation) error
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T, ctx context.Context) setupData
	}{
		{
			desc: "no payload",
			setup: func(t *testing.T, ctx context.Context) setupData {
				return setupData{
					env:         []string{},
					ctx:         ctx,
					expectedErr: fmt.Errorf("extracting hooks payload: %w", errors.New("no hooks payload found in environment")),
				}
			},
		},
		{
			desc: "invalid version",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err = pktline.WriteString(&stdin, "version=2")
				require.NoError(t, err)

				return setupData{
					env:         []string{payload},
					ctx:         ctx,
					stdin:       stdin.String(),
					expectedErr: errors.New("unsupported version: version=2"),
				}
			},
		},
		{
			desc: "single reference with atomic",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err = pktline.WriteString(&stdin, "version=1\000push-options atomic")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000atomic")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				_, err = pktline.WriteString(&stdout, "ok refs/heads/main")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)

				return setupData{
					env:            []string{payload},
					ctx:            ctx,
					stdin:          stdin.String(),
					expectedStdout: stdout.String(),
					expectedAtomic: true,
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					invocationSteps: func(invocation ProcReceiveHookInvocation) error {
						require.NoError(t, invocation.AcceptUpdate("refs/heads/main"))
						return invocation.Close()
					},
				}
			},
		},
		{
			desc: "single reference without atomic",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err = pktline.WriteString(&stdin, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				_, err = pktline.WriteString(&stdout, "ok refs/heads/main")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)

				return setupData{
					env:            []string{payload},
					ctx:            ctx,
					stdin:          stdin.String(),
					expectedStdout: stdout.String(),
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					invocationSteps: func(invocation ProcReceiveHookInvocation) error {
						require.NoError(t, invocation.AcceptUpdate("refs/heads/main"))
						return invocation.Close()
					},
				}
			},
		},
		{
			desc: "multiple references",
			setup: func(t *testing.T, ctx context.Context) setupData {
				var stdin bytes.Buffer
				_, err = pktline.WriteString(&stdin, "version=1\000push-options")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/main"))
				require.NoError(t, err)
				_, err = pktline.WriteString(&stdin, fmt.Sprintf("%s %s %s",
					gittest.DefaultObjectHash.ZeroOID, gittest.DefaultObjectHash.EmptyTreeOID, "refs/heads/branch"))
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdin)
				require.NoError(t, err)

				var stdout bytes.Buffer
				_, err = pktline.WriteString(&stdout, "version=1\000")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)
				_, err = pktline.WriteString(&stdout, "ok refs/heads/main")
				_, err = pktline.WriteString(&stdout, "ng refs/heads/branch for fun")
				require.NoError(t, err)
				err = pktline.WriteFlush(&stdout)

				return setupData{
					env:            []string{payload},
					ctx:            ctx,
					stdin:          stdin.String(),
					expectedStdout: stdout.String(),
					expectedUpdates: []ReferenceUpdate{
						{
							Ref:    "refs/heads/main",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
						{
							Ref:    "refs/heads/branch",
							OldOID: gittest.DefaultObjectHash.ZeroOID,
							NewOID: gittest.DefaultObjectHash.EmptyTreeOID,
						},
					},
					invocationSteps: func(invocation ProcReceiveHookInvocation) error {
						require.NoError(t, invocation.AcceptUpdate("refs/heads/main"))
						require.NoError(t, invocation.RejectUpdate("refs/heads/branch", "for fun"))
						return invocation.Close()
					},
				}
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			setup := tc.setup(t, ctx)

			var stdout, stderr bytes.Buffer
			err := hookManager.ProcReceiveHook(setup.ctx, repo, setup.env, strings.NewReader(setup.stdin), &stdout, &stderr)
			if err != nil || setup.expectedErr != nil {
				require.Equal(t, setup.expectedErr, err)
				return
			}

			invocation := procReceiveRegistry.Get(1)
			require.Equal(t, setup.expectedAtomic, invocation.Atomic())

			updates := invocation.ReferenceUpdates()
			require.Equal(t, setup.expectedUpdates, updates)

			require.NoError(t, setup.invocationSteps(invocation))
			require.Equal(t, setup.expectedStdout, stdout.String())
		})
	}
}
