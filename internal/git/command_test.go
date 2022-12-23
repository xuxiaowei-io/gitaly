package git_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
)

func TestExecOpts(t *testing.T) {
	cfg, _, repoPath := git.Setup(t)

	t.Run("default config", func(t *testing.T) {
		toplevel := git.ExecOpts(t, cfg, git.ExecConfig{}, "-C", repoPath, "rev-parse", "--path-format=absolute", "--git-dir")
		require.Equal(t, repoPath, text.ChompBytes(toplevel))
	})

	t.Run("env", func(t *testing.T) {
		configValue := git.ExecOpts(t, cfg, git.ExecConfig{
			Env: []string{
				"GIT_CONFIG_COUNT=1",
				"GIT_CONFIG_KEY_0=injected.env-config",
				"GIT_CONFIG_VALUE_0=some value",
			},
		}, "-C", repoPath, "config", "injected.env-config")

		require.Equal(t, "some value", text.ChompBytes(configValue))
	})

	t.Run("stdin", func(t *testing.T) {
		blobID := git.ExecOpts(t, cfg, git.ExecConfig{
			Stdin: strings.NewReader("blob contents"),
		}, "-C", repoPath, "hash-object", "-w", "--stdin")

		require.Equal(t,
			"blob contents",
			string(git.Exec(t, cfg, "-C", repoPath, "cat-file", "-p", text.ChompBytes(blobID))),
		)
	})

	t.Run("stdout", func(t *testing.T) {
		var buf bytes.Buffer
		git.ExecOpts(t, cfg, git.ExecConfig{
			Stdout: &buf,
		}, "-C", repoPath, "rev-parse", "--path-format=absolute", "--git-dir")

		require.Equal(t, repoPath, text.ChompBytes(buf.Bytes()))
	})
}
