//go:build !gitaly_test_sha256

package git_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
)

func TestHooksPayload(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	tx := txinfo.Transaction{
		ID:      1234,
		Node:    "primary",
		Primary: true,
	}

	t.Run("envvar has proper name", func(t *testing.T) {
		env, err := NewHooksPayload(cfg, repo, nil, nil, AllHooks, nil).Env()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(env, EnvHooksPayload+"="))
	})

	t.Run("roundtrip succeeds", func(t *testing.T) {
		env, err := NewHooksPayload(cfg, repo, nil, nil, PreReceiveHook, map[featureflag.FeatureFlag]bool{
			{Name: "flag_key"}: true,
		}).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{
			"UNRELATED=value",
			env,
			"ANOTHOR=unrelated-value",
			EnvHooksPayload + "_WITH_SUFFIX=is-ignored",
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:           repo,
			RuntimeDir:     cfg.RuntimeDir,
			InternalSocket: cfg.InternalSocketPath(),
			RequestedHooks: PreReceiveHook,
			FeatureFlagsWithValue: []FeatureFlagWithValue{
				{
					Flag:    featureflag.FeatureFlag{Name: "flag_key"},
					Enabled: true,
				},
			},
		}, payload)
	})

	t.Run("roundtrip with transaction succeeds", func(t *testing.T) {
		env, err := NewHooksPayload(cfg, repo, &tx, nil, UpdateHook, nil).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{env})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:           repo,
			RuntimeDir:     cfg.RuntimeDir,
			InternalSocket: cfg.InternalSocketPath(),
			Transaction:    &tx,
			RequestedHooks: UpdateHook,
		}, payload)
	})

	t.Run("missing envvar", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{"OTHER_ENV=foobar"})
		require.Error(t, err)
		require.Equal(t, ErrPayloadNotFound, err)
	})

	t.Run("bogus value", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{EnvHooksPayload + "=foobar"})
		require.Error(t, err)
	})

	t.Run("receive hooks payload", func(t *testing.T) {
		env, err := NewHooksPayload(cfg, repo, nil, &UserDetails{
			UserID:   "1234",
			Username: "user",
			Protocol: "ssh",
		}, PostReceiveHook, nil).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{
			env,
			"GL_ID=wrong",
			"GL_USERNAME=wrong",
			"GL_PROTOCOL=wrong",
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			RuntimeDir:          cfg.RuntimeDir,
			InternalSocket:      cfg.InternalSocketPath(),
			InternalSocketToken: cfg.Auth.Token,
			UserDetails: &UserDetails{
				UserID:   "1234",
				Username: "user",
				Protocol: "ssh",
			},
			RequestedHooks: PostReceiveHook,
		}, payload)
	})
}

func TestHooksPayload_IsHookRequested(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		configured Hook
		request    Hook
		expected   bool
	}{
		{
			desc:       "exact match",
			configured: PreReceiveHook,
			request:    PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "hook matches a set",
			configured: PreReceiveHook | PostReceiveHook,
			request:    PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "no match",
			configured: PreReceiveHook,
			request:    PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with a set",
			configured: PreReceiveHook | UpdateHook,
			request:    PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with nothing set",
			configured: 0,
			request:    PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "pre-receive hook with AllHooks",
			configured: AllHooks,
			request:    PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "post-receive hook with AllHooks",
			configured: AllHooks,
			request:    PostReceiveHook,
			expected:   true,
		},
		{
			desc:       "update hook with AllHooks",
			configured: AllHooks,
			request:    UpdateHook,
			expected:   true,
		},
		{
			desc:       "reference-transaction hook with AllHooks",
			configured: AllHooks,
			request:    ReferenceTransactionHook,
			expected:   true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual := HooksPayload{
				RequestedHooks: tc.configured,
			}.IsHookRequested(tc.request)
			require.Equal(t, tc.expected, actual)
		})
	}
}
