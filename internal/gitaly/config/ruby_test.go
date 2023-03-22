package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/errors/cfgerror"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestRuby_Validate(t *testing.T) {
	t.Parallel()

	tmpDir := testhelper.TempDir(t)
	tmpFile := filepath.Join(tmpDir, "file")
	require.NoError(t, os.WriteFile(tmpFile, nil, perm.SharedFile))

	for _, tc := range []struct {
		name        string
		ruby        Ruby
		expectedErr error
	}{
		{
			name: "all set valid",
			ruby: Ruby{
				Dir:                    tmpDir,
				GracefulRestartTimeout: duration.Duration(1),
				RestartDelay:           duration.Duration(1),
			},
		},
		{
			name: "dir is a relative path",
			ruby: Ruby{Dir: "."},
		},
		{
			name: "invalid",
			ruby: Ruby{
				Dir:                    "/does/not/exist",
				GracefulRestartTimeout: duration.Duration(-1),
				RestartDelay:           duration.Duration(-2),
			},
			expectedErr: cfgerror.ValidationErrors{
				cfgerror.NewValidationError(
					fmt.Errorf("%w: -1ns", cfgerror.ErrIsNegative),
					"graceful_restart_timeout",
				),
				cfgerror.NewValidationError(
					fmt.Errorf("%w: -2ns", cfgerror.ErrIsNegative),
					"restart_delay",
				),
				cfgerror.NewValidationError(
					fmt.Errorf("%w: %q", cfgerror.ErrDoesntExist, "/does/not/exist"),
					"dir",
				),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ruby.Validate()
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
