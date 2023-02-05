package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestCfg_ConfigureRuby(t *testing.T) {
	t.Parallel()
	tmpDir := testhelper.TempDir(t)

	tmpFile := filepath.Join(tmpDir, "file")
	require.NoError(t, os.WriteFile(tmpFile, nil, perm.SharedFile))

	testCases := []struct {
		desc        string
		rubyCfg     Ruby
		expectedErr error
	}{
		{
			desc:    "relative path",
			rubyCfg: Ruby{Dir: "."},
		},
		{
			desc:    "ok",
			rubyCfg: Ruby{Dir: tmpDir},
		},
		{
			desc:    "empty",
			rubyCfg: Ruby{},
			expectedErr: ValidationErrors{
				{Key: []string{"gitaly-ruby", "dir"}, Message: "is not set"},
			},
		},
		{
			desc: "invalid",
			rubyCfg: Ruby{
				Dir:                    "/does/not/exist",
				GracefulRestartTimeout: duration.Duration(-1),
				RestartDelay:           duration.Duration(-1),
				// Even though the path doesn't exist it doesn't produce an error
				// as there is no check for the directory existence.
				RuggedGitConfigSearchPath: "/does/not/exist/",
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"gitaly-ruby", "graceful_restart_timeout"},
					Message: "cannot be negative",
				},
				{
					Key:     []string{"gitaly-ruby", "restart_delay"},
					Message: "cannot be negative",
				},
				{
					Key:     []string{"gitaly-ruby", "dir"},
					Message: `'/does/not/exist' dir doesn't exist`,
				},
			},
		},
		{
			desc:    "exists but is not a directory",
			rubyCfg: Ruby{Dir: tmpFile},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"gitaly-ruby", "dir"},
					Message: fmt.Sprintf("'%s' is not a dir", tmpFile),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{Ruby: tc.rubyCfg}

			errs := cfg.ConfigureRuby()
			require.Equal(t, tc.expectedErr, errs)
			if tc.expectedErr != nil {
				return
			}

			dir := cfg.Ruby.Dir
			require.True(t, filepath.IsAbs(dir), "expected %q to be absolute path", dir)
		})
	}
}

func TestCfg_ConfigureRuby_numWorkers(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		in, out uint
	}{
		{in: 0, out: 2},
		{in: 1, out: 2},
		{in: 2, out: 2},
		{in: 3, out: 3},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			cfg := Cfg{Ruby: Ruby{Dir: "/", NumWorkers: tc.in}}
			require.Empty(t, cfg.ConfigureRuby())
			require.Equal(t, tc.out, cfg.Ruby.NumWorkers)
		})
	}
}
