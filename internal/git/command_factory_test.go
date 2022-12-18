//go:build !gitaly_test_sha256

package git_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestGitCommandProxy(t *testing.T) {
	cfg := testcfg.Build(t)

	requestReceived := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
	}))
	defer ts.Close()

	t.Setenv("http_proxy", ts.URL)

	ctx := testhelper.Context(t)

	dir := testhelper.TempDir(t)

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	defer cleanup()

	cmd, err := gitCmdFactory.NewWithoutRepo(ctx, git.Command{
		Name: "clone",
		Args: []string{"http://gitlab.com/bogus-repo", dir},
	}, git.WithDisabledHooks())
	require.NoError(t, err)

	err = cmd.Wait()
	require.NoError(t, err)
	require.True(t, requestReceived)
}

// Global git configuration is only disabled in tests for now. Gitaly should stop using the global
// git configuration in 15.0. See https://gitlab.com/gitlab-org/gitaly/-/issues/3617.
func TestExecCommandFactory_globalGitConfigIgnored(t *testing.T) {
	cfg := testcfg.Build(t)

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	defer cleanup()

	tmpHome := testhelper.TempDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(tmpHome, ".gitconfig"), []byte(`[ignored]
	value = true
`,
	), os.ModePerm))
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc   string
		filter string
	}{
		{desc: "global", filter: "--global"},
		// The test doesn't override the system config as that would be a global change or would
		// require chrooting. The assertion won't catch problems on systems that do not have system
		// level configuration set.
		{desc: "system", filter: "--system"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd, err := gitCmdFactory.NewWithoutRepo(ctx, git.Command{
				Name:  "config",
				Flags: []git.Option{git.Flag{Name: "--list"}, git.Flag{Name: tc.filter}},
			}, git.WithEnv("HOME="+tmpHome))
			require.NoError(t, err)

			configContents, err := io.ReadAll(cmd)
			require.NoError(t, err)
			require.NoError(t, cmd.Wait())
			require.Empty(t, string(configContents))
		})
	}
}

func TestExecCommandFactory_gitConfiguration(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	require.NoError(t, os.Remove(filepath.Join(repoPath, "config")))

	defaultConfig := func() []string {
		commandFactory, cleanup, err := git.NewExecCommandFactory(cfg)
		require.NoError(t, err)
		defer cleanup()

		globalConfig, err := commandFactory.GlobalConfiguration(ctx)
		require.NoError(t, err)

		var configEntries []string
		for _, config := range globalConfig {
			configEntries = append(configEntries, fmt.Sprintf(
				"%s=%s", strings.ToLower(config.Key), config.Value,
			))
		}
		return configEntries
	}

	for _, tc := range []struct {
		desc           string
		config         []config.GitConfig
		options        []git.CmdOpt
		expectedConfig []string
	}{
		{
			desc:           "without config",
			expectedConfig: defaultConfig(),
		},
		{
			desc: "config with simple entry",
			config: []config.GitConfig{
				{Key: "core.foo", Value: "bar"},
			},
			expectedConfig: append(defaultConfig(), "core.foo=bar"),
		},
		{
			desc: "config with empty value",
			config: []config.GitConfig{
				{Key: "core.empty", Value: ""},
			},
			expectedConfig: append(defaultConfig(), "core.empty="),
		},
		{
			desc: "config with subsection",
			config: []config.GitConfig{
				{Key: "http.http://example.com.proxy", Value: "http://proxy.example.com"},
			},
			expectedConfig: append(defaultConfig(), "http.http://example.com.proxy=http://proxy.example.com"),
		},
		{
			desc: "config with multiple keys",
			config: []config.GitConfig{
				{Key: "core.foo", Value: "initial"},
				{Key: "core.foo", Value: "second"},
			},
			expectedConfig: append(defaultConfig(), "core.foo=initial", "core.foo=second"),
		},
		{
			desc: "option",
			options: []git.CmdOpt{
				git.WithConfig(git.ConfigPair{Key: "core.foo", Value: "bar"}),
			},
			expectedConfig: append(defaultConfig(), "core.foo=bar"),
		},
		{
			desc: "multiple options",
			options: []git.CmdOpt{
				git.WithConfig(
					git.ConfigPair{Key: "core.foo", Value: "initial"},
					git.ConfigPair{Key: "core.foo", Value: "second"},
				),
			},
			expectedConfig: append(defaultConfig(), "core.foo=initial", "core.foo=second"),
		},
		{
			desc: "config comes after options",
			config: []config.GitConfig{
				{Key: "from.config", Value: "value"},
			},
			options: []git.CmdOpt{
				git.WithConfig(
					git.ConfigPair{Key: "from.option", Value: "value"},
				),
			},
			expectedConfig: append(defaultConfig(), "from.option=value", "from.config=value"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg.Git.Config = tc.config

			commandFactory, cleanup, err := git.NewExecCommandFactory(cfg)
			require.NoError(t, err)
			defer cleanup()

			var stdout bytes.Buffer
			cmd, err := commandFactory.New(ctx, repo, git.Command{
				Name: "config",
				Flags: []git.Option{
					git.Flag{Name: "--list"},
				},
			}, append(tc.options, git.WithStdout(&stdout))...)
			require.NoError(t, err)
			require.NoError(t, cmd.Wait())
			require.Equal(t, tc.expectedConfig, strings.Split(text.ChompBytes(stdout.Bytes()), "\n"))
		})
	}
}

func TestCommandFactory_ExecutionEnvironment(t *testing.T) {
	testhelper.Unsetenv(t, "GITALY_TESTING_GIT_BINARY")
	testhelper.Unsetenv(t, "GITALY_TESTING_BUNDLED_GIT_PATH")

	ctx := testhelper.Context(t)

	assertExecEnv := func(t *testing.T, cfg config.Cfg, expectedExecEnv git.ExecutionEnvironment) {
		t.Helper()
		gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, git.WithSkipHooks())
		require.NoError(t, err)
		defer cleanup()

		// We need to compare the execution environments manually because they also have
		// some private variables which we cannot easily check here.
		actualExecEnv := gitCmdFactory.GetExecutionEnvironment(ctx)
		require.Equal(t, expectedExecEnv.BinaryPath, actualExecEnv.BinaryPath)
		require.Equal(t, expectedExecEnv.EnvironmentVariables, actualExecEnv.EnvironmentVariables)
	}

	t.Run("set in config without ignored gitconfig", func(t *testing.T) {
		assertExecEnv(t, config.Cfg{
			Git: config.Git{
				BinPath:         "/path/to/myGit",
				IgnoreGitconfig: false,
			},
		}, git.ExecutionEnvironment{
			BinaryPath: "/path/to/myGit",
			EnvironmentVariables: []string{
				"LANG=en_US.UTF-8",
				"GIT_TERMINAL_PROMPT=0",
			},
		})
	})

	t.Run("set in config", func(t *testing.T) {
		assertExecEnv(t, config.Cfg{
			Git: config.Git{
				BinPath:         "/path/to/myGit",
				IgnoreGitconfig: true,
			},
		}, git.ExecutionEnvironment{
			BinaryPath: "/path/to/myGit",
			EnvironmentVariables: []string{
				"LANG=en_US.UTF-8",
				"GIT_TERMINAL_PROMPT=0",
				"GIT_CONFIG_GLOBAL=/dev/null",
				"GIT_CONFIG_SYSTEM=/dev/null",
				"XDG_CONFIG_HOME=/dev/null",
			},
		})
	})

	t.Run("set using GITALY_TESTING_GIT_BINARY", func(t *testing.T) {
		t.Setenv("GITALY_TESTING_GIT_BINARY", "/path/to/env_git")

		assertExecEnv(t, config.Cfg{
			Git: config.Git{
				IgnoreGitconfig: true,
			},
		}, git.ExecutionEnvironment{
			BinaryPath: "/path/to/env_git",
			EnvironmentVariables: []string{
				"NO_SET_GIT_TEMPLATE_DIR=YesPlease",
				"LANG=en_US.UTF-8",
				"GIT_TERMINAL_PROMPT=0",
				"GIT_CONFIG_GLOBAL=/dev/null",
				"GIT_CONFIG_SYSTEM=/dev/null",
				"XDG_CONFIG_HOME=/dev/null",
			},
		})
	})

	t.Run("set using GITALY_TESTING_BUNDLED_GIT_PATH", func(t *testing.T) {
		suffixes := []string{"-v2.38"}
		bundledGitDir := testhelper.TempDir(t)

		var bundledGitConstructors []git.BundledGitEnvironmentConstructor
		for _, constructor := range git.ExecutionEnvironmentConstructors {
			bundledGitConstructor, ok := constructor.(git.BundledGitEnvironmentConstructor)
			if ok {
				bundledGitConstructors = append(bundledGitConstructors, bundledGitConstructor)
			}
		}
		require.NotEmpty(t, bundledGitConstructors)

		var bundledGitExecutables, bundledGitRemoteExecutables []string
		for _, suffix := range suffixes {
			bundledGitExecutables = append(bundledGitExecutables, filepath.Join(bundledGitDir, "gitaly-git"+suffix))
			bundledGitRemoteExecutables = append(bundledGitRemoteExecutables, filepath.Join(bundledGitDir, "gitaly-git-remote-http"+suffix))
		}

		t.Setenv("GITALY_TESTING_BUNDLED_GIT_PATH", bundledGitDir)

		t.Run("missing bin_dir", func(t *testing.T) {
			_, _, err := git.NewExecCommandFactory(config.Cfg{Git: config.Git{}}, git.WithSkipHooks())
			require.EqualError(t, err, "setting up Git execution environment: constructing Git environment: cannot use bundled binaries without bin path being set")
		})

		t.Run("missing gitaly-git executable", func(t *testing.T) {
			_, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: testhelper.TempDir(t)}, git.WithSkipHooks())
			require.Error(t, err)
			require.Contains(t, err.Error(), `statting "gitaly-git-v2.38"`)
			require.True(t, errors.Is(err, os.ErrNotExist))
		})

		t.Run("missing git-remote-http executable", func(t *testing.T) {
			for _, bundledGitExecutable := range bundledGitExecutables {
				require.NoError(t, os.WriteFile(bundledGitExecutable, []byte{}, 0o777))
			}

			_, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: testhelper.TempDir(t)}, git.WithSkipHooks())
			require.Error(t, err)
			require.Contains(t, err.Error(), `statting "gitaly-git-remote-http-v2.38"`)
			require.True(t, errors.Is(err, os.ErrNotExist))
		})

		t.Run("missing git-http-backend executable", func(t *testing.T) {
			for _, bundledGitExecutable := range bundledGitExecutables {
				require.NoError(t, os.WriteFile(bundledGitExecutable, []byte{}, 0o777))
			}
			for _, bundledGitRemoteExecutable := range bundledGitRemoteExecutables {
				require.NoError(t, os.WriteFile(bundledGitRemoteExecutable, []byte{}, 0o777))
			}

			_, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: testhelper.TempDir(t)}, git.WithSkipHooks())
			require.Error(t, err)
			require.Contains(t, err.Error(), `statting "gitaly-git-http-backend-v2.38"`)
			require.True(t, errors.Is(err, os.ErrNotExist))
		})

		t.Run("bin_dir with executables", func(t *testing.T) {
			expectedSuffix := "-v2.38"

			for _, bundledGitConstructor := range bundledGitConstructors {
				for _, gitBinary := range []string{"gitaly-git", "gitaly-git-remote-http", "gitaly-git-http-backend"} {
					require.NoError(t, os.WriteFile(filepath.Join(bundledGitDir, gitBinary+bundledGitConstructor.Suffix), []byte{}, 0o777))
				}
			}

			binDir := testhelper.TempDir(t)

			gitCmdFactory, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: binDir}, git.WithSkipHooks())
			require.NoError(t, err)

			gitBinPath := gitCmdFactory.GetExecutionEnvironment(ctx).BinaryPath

			for _, executable := range []string{"git", "git-remote-http", "git-http-backend"} {
				symlinkPath := filepath.Join(filepath.Dir(gitBinPath), executable)

				// The symlink in Git's temporary BinPath points to the Git
				// executable in Gitaly's BinDir.
				target, err := os.Readlink(symlinkPath)
				require.NoError(t, err)
				require.Equal(t, filepath.Join(binDir, "gitaly-"+executable+expectedSuffix), target)

				// And in a test setup, the symlink in Gitaly's BinDir must point to
				// the Git binary pointed to by the GITALY_TESTING_BUNDLED_GIT_PATH
				// environment variable.
				target, err = os.Readlink(target)
				require.NoError(t, err)
				require.Equal(t, filepath.Join(bundledGitDir, "gitaly-"+executable+expectedSuffix), target)
			}
		})
	})

	t.Run("not set, get from system", func(t *testing.T) {
		resolvedPath, err := exec.LookPath("git")
		require.NoError(t, err)

		assertExecEnv(t, config.Cfg{
			Git: config.Git{
				IgnoreGitconfig: true,
			},
		}, git.ExecutionEnvironment{
			BinaryPath: resolvedPath,
			EnvironmentVariables: []string{
				"LANG=en_US.UTF-8",
				"GIT_TERMINAL_PROMPT=0",
				"GIT_CONFIG_GLOBAL=/dev/null",
				"GIT_CONFIG_SYSTEM=/dev/null",
				"XDG_CONFIG_HOME=/dev/null",
			},
		})
	})

	t.Run("doesn't exist in the system", func(t *testing.T) {
		testhelper.Unsetenv(t, "PATH")

		_, _, err := git.NewExecCommandFactory(config.Cfg{}, git.WithSkipHooks())
		require.EqualError(t, err, "setting up Git execution environment: could not set up any Git execution environments")
	})
}

func TestExecCommandFactoryHooksPath(t *testing.T) {
	ctx := testhelper.Context(t)

	t.Run("temporary hooks", func(t *testing.T) {
		cfg := config.Cfg{
			BinDir: testhelper.TempDir(t),
		}

		t.Run("no overrides", func(t *testing.T) {
			gitCmdFactory := gittest.NewCommandFactory(t, cfg)

			hooksPath := gitCmdFactory.HooksPath(ctx)

			// We cannot assert that the hooks path is equal to any specific
			// string, but instead we can assert that it exists and contains the
			// symlinks we expect.
			for _, hook := range []string{"update", "pre-receive", "post-receive", "reference-transaction"} {
				target, err := os.Readlink(filepath.Join(hooksPath, hook))
				require.NoError(t, err)
				require.Equal(t, cfg.BinaryPath("gitaly-hooks"), target)
			}
		})

		t.Run("with skip", func(t *testing.T) {
			gitCmdFactory := gittest.NewCommandFactory(t, cfg, git.WithSkipHooks())
			require.Equal(t, "/var/empty", gitCmdFactory.HooksPath(ctx))
		})
	})

	t.Run("hooks path", func(t *testing.T) {
		gitCmdFactory := gittest.NewCommandFactory(t, config.Cfg{
			BinDir: testhelper.TempDir(t),
		}, git.WithHooksPath("/hooks/path"))

		// The environment variable shouldn't override an explicitly set hooks path.
		require.Equal(t, "/hooks/path", gitCmdFactory.HooksPath(ctx))
	})
}

func TestExecCommandFactory_GitVersion(t *testing.T) {
	ctx := testhelper.Context(t)

	generateVersionScript := func(version string) func(git.ExecutionEnvironment) string {
		return func(git.ExecutionEnvironment) string {
			return fmt.Sprintf(
				`#!/usr/bin/env bash
				echo '%s'
			`, version)
		}
	}

	for _, tc := range []struct {
		desc            string
		versionString   string
		expectedErr     string
		expectedVersion string
	}{
		{
			desc:            "valid version",
			versionString:   "git version 2.33.1.gl1",
			expectedVersion: "2.33.1.gl1",
		},
		{
			desc:            "valid version with trailing newline",
			versionString:   "git version 2.33.1.gl1\n",
			expectedVersion: "2.33.1.gl1",
		},
		{
			desc:          "multi-line version",
			versionString: "git version 2.33.1.gl1\nfoobar\n",
			expectedErr:   "cannot parse git version: strconv.ParseUint: parsing \"1\\nfoobar\": invalid syntax",
		},
		{
			desc:          "unexpected format",
			versionString: "2.33.1\n",
			expectedErr:   "invalid version format: \"2.33.1\\n\\n\"",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			gitCmdFactory := gittest.NewInterceptingCommandFactory(
				t, ctx, testcfg.Build(t), generateVersionScript(tc.versionString),
				gittest.WithRealCommandFactoryOptions(git.WithSkipHooks()),
				gittest.WithInterceptedVersion(),
			)

			actualVersion, err := gitCmdFactory.GitVersion(ctx)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedVersion, actualVersion.String())
		})
	}

	t.Run("caching", func(t *testing.T) {
		gitCmdFactory := gittest.NewInterceptingCommandFactory(
			t, ctx, testcfg.Build(t), generateVersionScript("git version 1.2.3"),
			gittest.WithRealCommandFactoryOptions(git.WithSkipHooks()),
			gittest.WithInterceptedVersion(),
		)

		gitPath := gitCmdFactory.GetExecutionEnvironment(ctx).BinaryPath
		stat, err := os.Stat(gitPath)
		require.NoError(t, err)

		version, err := gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "1.2.3", version.String())

		// We rewrite the file with the same content length and modification time such that
		// its file information doesn't change. As a result, information returned by
		// stat(3P) shouldn't differ and we should continue to see the cached version. This
		// is a known insufficiency, but it is extremely unlikely to ever happen in
		// production when the real Git binary changes.
		require.NoError(t, os.Remove(gitPath))
		testhelper.WriteExecutable(t, gitPath, []byte(generateVersionScript("git version 9.8.7")(git.ExecutionEnvironment{})))
		require.NoError(t, os.Chtimes(gitPath, stat.ModTime(), stat.ModTime()))

		// Given that we continue to use the cached version we shouldn't see any
		// change here.
		version, err = gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "1.2.3", version.String())

		// If we really replace the Git binary with something else, then we should
		// see a changed version.
		require.NoError(t, os.Remove(gitPath))
		testhelper.WriteExecutable(t, gitPath, []byte(
			`#!/usr/bin/env bash
			echo 'git version 2.34.1'
		`))

		version, err = gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "2.34.1", version.String())
	})
}

func TestExecCommandFactory_config(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	// Create a repository and remove its gitconfig to bring us into a known state where there
	// is no repo-level configuration that interferes with our test.
	repo, repoDir := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	require.NoError(t, os.Remove(filepath.Join(repoDir, "config")))

	expectedEnv := []string{
		"gc.auto=0",
		"core.autocrlf=input",
		"core.usereplacerefs=false",
		"commitgraph.generationversion=1",
		"core.fsync=objects,derived-metadata,reference",
		"core.fsyncmethod=fsync",
	}

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	var stdout bytes.Buffer
	cmd, err := gitCmdFactory.New(ctx, repo, git.Command{
		Name: "config",
		Flags: []git.Option{
			git.Flag{Name: "--list"},
		},
	}, git.WithStdout(&stdout))
	require.NoError(t, err)

	require.NoError(t, cmd.Wait())
	require.Equal(t, expectedEnv, strings.Split(strings.TrimSpace(stdout.String()), "\n"))
}

func TestExecCommandFactory_SidecarGitConfiguration(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	cfg.Git.Config = []config.GitConfig{
		{Key: "custom.key", Value: "injected"},
	}

	commonHead := []git.ConfigPair{
		{Key: "gc.auto", Value: "0"},
		{Key: "core.autocrlf", Value: "input"},
		{Key: "core.useReplaceRefs", Value: "false"},
		{Key: "commitGraph.generationVersion", Value: "1"},
	}

	commonTail := []git.ConfigPair{
		{Key: "custom.key", Value: "injected"},
	}

	for _, tc := range []struct {
		desc           string
		version        string
		expectedConfig []git.ConfigPair
	}{
		{
			desc:    "with core.fsync",
			version: "2.36.0",
			expectedConfig: append(append(commonHead,
				git.ConfigPair{Key: "core.fsync", Value: "objects,derived-metadata,reference"},
				git.ConfigPair{Key: "core.fsyncMethod", Value: "fsync"},
			), commonTail...),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			factory := gittest.NewInterceptingCommandFactory(t, ctx, cfg, func(git.ExecutionEnvironment) string {
				return fmt.Sprintf(
					`#!/usr/bin/env bash
					echo "git version %s"
				`, tc.version)
			}, gittest.WithInterceptedVersion())

			configPairs, err := factory.SidecarGitConfiguration(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedConfig, configPairs)
		})
	}
}

// TestFsckConfiguration tests the hardcoded configuration of the
// git fsck subcommand generated through the command factory.
func TestFsckConfiguration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc string
		data string
	}{
		{
			desc: "with valid commit",
			data: strings.Join([]string{
				"tree " + gittest.DefaultObjectHash.EmptyTreeOID.String(),
				"author " + gittest.DefaultCommitterSignature,
				"committer " + gittest.DefaultCommitterSignature,
				"",
				"message",
			}, "\n"),
		},
		{
			desc: "with missing space",
			data: strings.Join([]string{
				"tree " + gittest.DefaultObjectHash.EmptyTreeOID.String(),
				"author Scrooge McDuck <scrooge@mcduck.com>1659043074 -0500",
				"committer Scrooge McDuck <scrooge@mcduck.com>1659975573 -0500",
				"",
				"message",
			}, "\n"),
		},
		{
			desc: "with bad timezone",
			data: strings.Join([]string{
				"tree " + gittest.DefaultObjectHash.EmptyTreeOID.String(),
				"author Scrooge McDuck <scrooge@mcduck.com> 1659043074 -0500BAD",
				"committer Scrooge McDuck <scrooge@mcduck.com> 1659975573 -0500BAD",
				"",
				"message",
			}, "\n"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
				gittest.CreateRepositoryConfig{SkipCreationViaService: true},
			)

			// Create commit object.
			commitOut := gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewBufferString(tc.data)},
				"-C", repoPath, "hash-object", "-w", "-t", "commit", "--stdin", "--literally",
			)
			_, err := gittest.DefaultObjectHash.FromHex(text.ChompBytes(commitOut))
			require.NoError(t, err)

			gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
			require.NoError(t, err)
			defer cleanup()

			// Create fsck command with configured ignore rules options.
			cmd, err := gitCmdFactory.New(ctx, repoProto,
				git.Command{Name: "fsck"},
			)
			require.NoError(t, err)

			// Execute git fsck command.
			err = cmd.Wait()
			require.NoError(t, err)
		})
	}
}

// TestExecCommandFactory_tooManyOpenFileDescriptors test exists to make sure the system
// returns an expected syscall.EMFILE error when there is a problem with FD as the
// middleware in internal/middleware/statushandler/statushandler.go heavily depends on it.
func TestExecCommandFactory_tooManyOpenFileDescriptors(t *testing.T) {
	// Do not run this test with t.Parallel(). The test setup affects settings of the
	// current process and may lead to other tests failures.
	ctx := testhelper.Context(t)

	// setCur changes value of the Cur field of the Rlimit type.
	// It is needed because on different OS this field represented by different types.
	// Currently only by two: uint64, int64. That is why we use reflection to change the value.
	setCur := func(t *testing.T, rl syscall.Rlimit, val int) syscall.Rlimit {
		var cur interface{} = rl.Cur
		switch cur.(type) {
		case uint64:
			reflect.ValueOf(&rl.Cur).Elem().SetUint(uint64(val))
		case int64:
			reflect.ValueOf(&rl.Cur).Elem().SetInt(int64(val))
		default:
			require.FailNowf(t, "code needs to be changed", "please add a new type: %T", rl.Cur)
		}
		return rl
	}

	setOpenFilesLimit := func(t *testing.T, limit int) {
		// This is a flag for the resource to limit. In that case: open file descriptors.
		const flag = syscall.RLIMIT_NOFILE
		var rlim syscall.Rlimit
		require.NoError(t, syscall.Getrlimit(flag, &rlim))
		t.Cleanup(func() {
			require.NoError(t, syscall.Setrlimit(flag, &rlim))
		})
		newRLimit := setCur(t, rlim, limit)
		require.NoError(t, syscall.Setrlimit(flag, &newRLimit))
	}

	cfg, repo, _ := testcfg.BuildWithRepo(t)
	factory, finish, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	t.Cleanup(finish)

	setOpenFilesLimit(t, 10)

	// It is enough to run whatever command usually as the limit is really low.
	cmd, err := factory.New(ctx, repo, git.Command{
		Name: "show",
		Args: []string{"HEAD"},
	})
	if !assert.Error(t, err) {
		_ = cmd.Wait() // we don't care about an error here as it shouldn't happen
		return
	}
	require.ErrorIs(t, err, syscall.EMFILE) // too many open files
}
