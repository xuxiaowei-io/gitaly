package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/version"
)

func TestGitalyCLI(t *testing.T) {
	cfg := testcfg.Build(t)
	binaryPath := testcfg.BuildGitaly(t, cfg)

	for _, tc := range []struct {
		desc     string
		args     []string
		exitCode int
		stdout   string
		stderr   string
	}{
		{
			desc:   "version invocation",
			args:   []string{"-version"},
			stdout: version.GetVersionString("Gitaly") + "\n",
		},
		{
			desc:     "without arguments",
			exitCode: 2,
			stdout:   fmt.Sprintf("Gitaly, version %s\nUsage: %s [command] [options] <configfile>\n\nThe commands are:\n\n\tcheck\tchecks accessability of internal Rails API\n", version.GetVersion(), binaryPath),
			stderr:   "  -version\n    \tPrint version and exit\n",
		},
		{
			desc:     "with non-existent config",
			args:     []string{"non-existent-file"},
			exitCode: 1,
			stdout:   `msg="load config: config_path \"non-existent-file\"`,
		},
		{
			desc:     "check without config",
			args:     []string{"check"},
			exitCode: 2,
			stderr:   fmt.Sprintf("error: invalid argument(s)Usage: %s check <configfile>\n", binaryPath),
		},
		{
			desc:     "check with non-existent config",
			args:     []string{"check", "non-existent-file"},
			exitCode: 1,
			stdout:   "Checking GitLab API access: FAILED",
			stderr:   "load config: config_path \"non-existent-file\": open non-existent-file: no such file or directory\n",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			var stdout, stderr bytes.Buffer
			cmd := exec.CommandContext(ctx, binaryPath, tc.args...)
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			err := cmd.Run()

			exitCode := 0
			if err != nil {
				exitCode = err.(*exec.ExitError).ExitCode()
			}

			assert.Equal(t, tc.exitCode, exitCode)
			if tc.stdout == "" {
				assert.Empty(t, stdout.String())
			}
			assert.Contains(t, stdout.String(), tc.stdout)

			if tc.stderr == "" {
				assert.Empty(t, stderr.String())
			}
			assert.Contains(t, stderr.String(), tc.stderr)
		})
	}
}
