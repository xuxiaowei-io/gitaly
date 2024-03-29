package git_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
)

func TestHooksPayload(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	tx := txinfo.Transaction{
		ID:      1234,
		Node:    "primary",
		Primary: true,
	}

	t.Run("envvar has proper name", func(t *testing.T) {
		env, err := git.NewHooksPayload(
			cfg,
			repo,
			gittest.DefaultObjectHash,
			nil,
			nil,
			git.AllHooks,
			nil,
			0,
		).Env()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(env, git.EnvHooksPayload+"="))
	})

	t.Run("roundtrip succeeds", func(t *testing.T) {
		env, err := git.NewHooksPayload(
			cfg,
			repo,
			gittest.DefaultObjectHash,
			nil,
			nil,
			git.PreReceiveHook,
			map[featureflag.FeatureFlag]bool{
				{Name: "flag_key"}: true,
			},
			1,
		).Env()
		require.NoError(t, err)

		payload, err := git.HooksPayloadFromEnv([]string{
			"UNRELATED=value",
			env,
			"ANOTHOR=unrelated-value",
			git.EnvHooksPayload + "_WITH_SUFFIX=is-ignored",
		})
		require.NoError(t, err)

		require.Equal(t, git.HooksPayload{
			Repo:           repo,
			ObjectFormat:   gittest.DefaultObjectHash.Format,
			RuntimeDir:     cfg.RuntimeDir,
			InternalSocket: cfg.InternalSocketPath(),
			RequestedHooks: git.PreReceiveHook,
			FeatureFlagsWithValue: []git.FeatureFlagWithValue{
				{
					Flag:    featureflag.FeatureFlag{Name: "flag_key"},
					Enabled: true,
				},
			},
			TransactionID: 1,
		}, payload)
	})

	t.Run("roundtrip with transaction succeeds", func(t *testing.T) {
		env, err := git.NewHooksPayload(
			cfg,
			repo,
			gittest.DefaultObjectHash,
			&tx,
			nil,
			git.UpdateHook,
			nil,
			0,
		).Env()
		require.NoError(t, err)

		payload, err := git.HooksPayloadFromEnv([]string{env})
		require.NoError(t, err)

		require.Equal(t, git.HooksPayload{
			Repo:           repo,
			ObjectFormat:   gittest.DefaultObjectHash.Format,
			RuntimeDir:     cfg.RuntimeDir,
			InternalSocket: cfg.InternalSocketPath(),
			Transaction:    &tx,
			RequestedHooks: git.UpdateHook,
		}, payload)
	})

	t.Run("missing envvar", func(t *testing.T) {
		_, err := git.HooksPayloadFromEnv([]string{"OTHER_ENV=foobar"})
		require.Error(t, err)
		require.Equal(t, git.ErrPayloadNotFound, err)
	})

	t.Run("bogus value", func(t *testing.T) {
		_, err := git.HooksPayloadFromEnv([]string{git.EnvHooksPayload + "=foobar"})
		require.Error(t, err)
	})

	t.Run("receive hooks payload", func(t *testing.T) {
		env, err := git.NewHooksPayload(
			cfg,
			repo,
			gittest.DefaultObjectHash,
			nil,
			&git.UserDetails{
				UserID:   "1234",
				Username: "user",
				Protocol: "ssh",
			}, git.PostReceiveHook, nil, 0).Env()
		require.NoError(t, err)

		payload, err := git.HooksPayloadFromEnv([]string{
			env,
			"GL_ID=wrong",
			"GL_USERNAME=wrong",
			"GL_PROTOCOL=wrong",
		})
		require.NoError(t, err)

		require.Equal(t, git.HooksPayload{
			Repo:                repo,
			ObjectFormat:        gittest.DefaultObjectHash.Format,
			RuntimeDir:          cfg.RuntimeDir,
			InternalSocket:      cfg.InternalSocketPath(),
			InternalSocketToken: cfg.Auth.Token,
			UserDetails: &git.UserDetails{
				UserID:   "1234",
				Username: "user",
				Protocol: "ssh",
			},
			RequestedHooks: git.PostReceiveHook,
		}, payload)
	})
}

func TestHooksPayload_IsHookRequested(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc       string
		configured git.Hook
		request    git.Hook
		expected   bool
	}{
		{
			desc:       "exact match",
			configured: git.PreReceiveHook,
			request:    git.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "hook matches a set",
			configured: git.PreReceiveHook | git.PostReceiveHook,
			request:    git.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "no match",
			configured: git.PreReceiveHook,
			request:    git.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with a set",
			configured: git.PreReceiveHook | git.UpdateHook,
			request:    git.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with nothing set",
			configured: 0,
			request:    git.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "pre-receive hook with AllHooks",
			configured: git.AllHooks,
			request:    git.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "post-receive hook with AllHooks",
			configured: git.AllHooks,
			request:    git.PostReceiveHook,
			expected:   true,
		},
		{
			desc:       "update hook with AllHooks",
			configured: git.AllHooks,
			request:    git.UpdateHook,
			expected:   true,
		},
		{
			desc:       "reference-transaction hook with AllHooks",
			configured: git.AllHooks,
			request:    git.ReferenceTransactionHook,
			expected:   true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual := git.HooksPayload{
				RequestedHooks: tc.configured,
			}.IsHookRequested(tc.request)
			require.Equal(t, tc.expected, actual)
		})
	}
}
