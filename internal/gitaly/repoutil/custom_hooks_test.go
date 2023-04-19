package repoutil

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestGetCustomHooks_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	expectedTarResponse := []string{
		"custom_hooks/",
		"custom_hooks/pre-commit.sample",
		"custom_hooks/prepare-commit-msg.sample",
		"custom_hooks/pre-push.sample",
	}
	require.NoError(t, os.Mkdir(filepath.Join(repoPath, "custom_hooks"), perm.PrivateDir), "Could not create custom_hooks dir")
	for _, fileName := range expectedTarResponse[1:] {
		require.NoError(t, os.WriteFile(filepath.Join(repoPath, fileName), []byte("Some hooks"), perm.PrivateExecutable), fmt.Sprintf("Could not create %s", fileName))
	}

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, locator, &hooks, repo))

	reader := tar.NewReader(&hooks)
	fileLength := 0
	for {
		file, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		fileLength++
		require.Contains(t, expectedTarResponse, file.Name)
	}
	require.Equal(t, fileLength, len(expectedTarResponse))
}

func TestGetCustomHooks_symlink(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	linkTarget := "/var/empty"
	require.NoError(t, os.Symlink(linkTarget, filepath.Join(repoPath, "custom_hooks")), "Could not create custom_hooks symlink")

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, locator, &hooks, repo))

	reader := tar.NewReader(&hooks)
	file, err := reader.Next()
	require.NoError(t, err)

	require.Equal(t, "custom_hooks", file.Name, "tar entry name")
	require.Equal(t, byte(tar.TypeSymlink), file.Typeflag, "tar entry type")
	require.Equal(t, linkTarget, file.Linkname, "link target")

	_, err = reader.Next()
	require.Equal(t, io.EOF, err, "custom_hooks should have been the only entry")
}

func TestGetCustomHooks_nonexistentHooks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)
	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	var hooks bytes.Buffer
	require.NoError(t, GetCustomHooks(ctx, locator, &hooks, repo))

	reader := tar.NewReader(&hooks)
	buf := bytes.NewBuffer(nil)
	_, err := io.Copy(buf, reader)
	require.NoError(t, err)

	require.Empty(t, buf.String(), "Returned stream should be empty")
}
