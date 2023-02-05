package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/duration"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestBinaryPath(t *testing.T) {
	cfg := Cfg{
		BinDir:     "bindir",
		RuntimeDir: "runtime",
	}

	require.Equal(t, "runtime/gitaly-hooks", cfg.BinaryPath("gitaly-hooks"))
	require.Equal(t, "bindir/gitaly-debug", cfg.BinaryPath("gitaly-debug"))
	require.Equal(t, "bindir", cfg.BinaryPath(""))
}

func TestLoadBrokenConfig(t *testing.T) {
	tmpFile := strings.NewReader(`path = "/tmp"\nname="foo"`)
	_, err := Load(tmpFile)
	assert.Error(t, err)
}

func TestLoadEmptyConfig(t *testing.T) {
	cfg, err := Load(strings.NewReader(``))
	require.NoError(t, err)

	expectedCfg := Cfg{
		Prometheus: prometheus.DefaultConfig(),
	}
	require.NoError(t, expectedCfg.setDefaults())

	// The runtime directory is a temporary path, so we need to take the value from the loaded
	// config. Furthermore, because `setDefaults()` would append the PID, we can't do so before
	// calling that function.
	expectedCfg.RuntimeDir = cfg.RuntimeDir

	require.Equal(t, expectedCfg, cfg)
}

func TestLoadURLs(t *testing.T) {
	tmpFile := strings.NewReader(`
[gitlab]
url = "unix:///tmp/test.socket"
relative_url_root = "/gitlab"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	expectedCfg := Cfg{
		Gitlab: Gitlab{
			URL:             "unix:///tmp/test.socket",
			RelativeURLRoot: "/gitlab",
		},
	}
	require.NoError(t, expectedCfg.setDefaults())
	require.Equal(t, expectedCfg.Gitlab, cfg.Gitlab)
}

func TestLoadStorage(t *testing.T) {
	tmpFile := strings.NewReader(`[[storage]]
name = "default"
path = "/tmp/"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	expectedCfg := Cfg{
		Storages: []Storage{
			{Name: "default", Path: "/tmp"},
		},
	}
	require.NoError(t, expectedCfg.setDefaults())
	require.Equal(t, expectedCfg.Storages, cfg.Storages)
}

func TestUncleanStoragePaths(t *testing.T) {
	cfg, err := Load(strings.NewReader(`[[storage]]
name="unclean-path-1"
path="/tmp/repos1//"

[[storage]]
name="unclean-path-2"
path="/tmp/repos2/subfolder/.."
`))
	require.NoError(t, err)

	require.Equal(t, []Storage{
		{Name: "unclean-path-1", Path: "/tmp/repos1"},
		{Name: "unclean-path-2", Path: "/tmp/repos2"},
	}, cfg.Storages)
}

func TestLoadMultiStorage(t *testing.T) {
	tmpFile := strings.NewReader(`[[storage]]
name="default"
path="/tmp/repos1"

[[storage]]
name="other"
path="/tmp/repos2/"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, []Storage{
		{Name: "default", Path: "/tmp/repos1"},
		{Name: "other", Path: "/tmp/repos2"},
	}, cfg.Storages)
}

func TestLoadSentry(t *testing.T) {
	tmpFile := strings.NewReader(`[logging]
sentry_environment = "production"
sentry_dsn = "abc123"
ruby_sentry_dsn = "xyz456"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, Logging{
		Sentry: Sentry(sentry.Config{
			Environment: "production",
			DSN:         "abc123",
		}),
		RubySentryDSN: "xyz456",
	}, cfg.Logging)
}

func TestLoadPrometheus(t *testing.T) {
	tmpFile := strings.NewReader(`
		prometheus_listen_addr=":9236"
		[prometheus]
		scrape_timeout       = "1s"
		grpc_latency_buckets = [0.0, 1.0, 2.0]
	`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, ":9236", cfg.PrometheusListenAddr)
	require.Equal(t, prometheus.Config{
		ScrapeTimeout:      duration.Duration(time.Second),
		GRPCLatencyBuckets: []float64{0, 1, 2},
	}, cfg.Prometheus)
}

func TestLoadSocketPath(t *testing.T) {
	tmpFile := strings.NewReader(`socket_path="/tmp/gitaly.sock"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, "/tmp/gitaly.sock", cfg.SocketPath)
}

func TestLoadListenAddr(t *testing.T) {
	tmpFile := strings.NewReader(`listen_addr=":8080"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)
	require.Equal(t, ":8080", cfg.ListenAddr)
}

func TestValidateStorages(t *testing.T) {
	repositories := testhelper.TempDir(t)
	repositories2 := testhelper.TempDir(t)
	nestedRepositories := filepath.Join(repositories, "nested")
	require.NoError(t, os.MkdirAll(nestedRepositories, perm.PublicDir))

	filePath := filepath.Join(testhelper.TempDir(t), "temporary-file")
	require.NoError(t, os.WriteFile(filePath, []byte{}, perm.PublicFile))

	invalidDir := filepath.Join(filepath.Dir(repositories), t.Name())

	testCases := []struct {
		desc        string
		storages    []Storage
		expectedErr error
	}{
		{
			desc: "no storages",
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage"},
					Message: "is not set",
				},
			},
		},
		{
			desc: "just 1 storage",
			storages: []Storage{
				{Name: "default", Path: repositories},
			},
		},
		{
			desc: "multiple storages",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "other", Path: repositories2},
			},
		},
		{
			desc: "multiple storages pointing to same directory",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "other", Path: repositories},
				{Name: "third", Path: repositories},
			},
		},
		{
			desc: "nested paths 1",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "other", Path: repositories},
				{Name: "third", Path: nestedRepositories},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "path"},
					Message: fmt.Sprintf("for storage 'third' at declaration 3 '%s': is nest with '%s' for storage 'default' at declaration 1", nestedRepositories, repositories),
				},
				{
					Key:     []string{"storage", "path"},
					Message: fmt.Sprintf("for storage 'third' at declaration 3 '%s': is nest with '%s' for storage 'other' at declaration 2", nestedRepositories, repositories),
				},
			},
		},
		{
			desc: "nested paths 2",
			storages: []Storage{
				{Name: "default", Path: nestedRepositories},
				{Name: "other", Path: repositories},
				{Name: "", Path: repositories},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "path"},
					Message: fmt.Sprintf("for storage 'other' at declaration 2 '%s': is nest with '%s' for storage 'default' at declaration 1", repositories, nestedRepositories),
				},
				{
					Key:     []string{"storage", "name"},
					Message: "empty value at declaration 3",
				},
				{
					Key:     []string{"storage", "path"},
					Message: fmt.Sprintf("at declaration 3 '%s': is nest with '%s' for storage 'default' at declaration 1", repositories, nestedRepositories),
				},
			},
		},
		{
			desc: "duplicate definition",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "default", Path: repositories},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "name"},
					Message: "'default' at declaration 2: is defined more than once",
				},
			},
		},
		{
			desc: "re-definition",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "default", Path: repositories2},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "name"},
					Message: "'default' at declaration 2: is defined more than once",
				},
			},
		},
		{
			desc: "empty name",
			storages: []Storage{
				{Name: "some", Path: repositories},
				{Name: "", Path: repositories},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "name"},
					Message: "empty value at declaration 2",
				},
			},
		},
		{
			desc: "empty path",
			storages: []Storage{
				{Name: "some", Path: repositories},
				{Name: "default", Path: ""},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "path"},
					Message: "empty value at declaration 2",
				},
			},
		},
		{
			desc: "non existing directory",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "nope", Path: invalidDir},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "path"},
					Message: fmt.Sprintf("for storage 'nope' at declaration 2: '%s' dir doesn't exist", invalidDir),
				},
			},
		},
		{
			desc: "path points to the regular file",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "is_file", Path: filePath},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"storage", "path"},
					Message: fmt.Sprintf("for storage 'is_file' at declaration 2: '%s' is not a dir", filePath),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{Storages: tc.storages}

			errs := cfg.validateStorages()
			assert.Equal(t, tc.expectedErr, errs)
		})
	}
}

func TestStoragePath(t *testing.T) {
	cfg := Cfg{Storages: []Storage{
		{Name: "default", Path: "/home/git/repositories1"},
		{Name: "other", Path: "/home/git/repositories2"},
		{Name: "third", Path: "/home/git/repositories3"},
	}}

	testCases := []struct {
		in, out string
		ok      bool
	}{
		{in: "default", out: "/home/git/repositories1", ok: true},
		{in: "third", out: "/home/git/repositories3", ok: true},
		{in: "", ok: false},
		{in: "foobar", ok: false},
	}

	for _, tc := range testCases {
		out, ok := cfg.StoragePath(tc.in)
		if !assert.Equal(t, tc.ok, ok, "%+v", tc) {
			continue
		}
		assert.Equal(t, tc.out, out, "%+v", tc)
	}
}

func TestLoadGit(t *testing.T) {
	tmpFile := strings.NewReader(`[git]
bin_path = "/my/git/path"
catfile_cache_size = 50

[[git.config]]
key = "first.key"
value = "first-value"

[[git.config]]
key = "second.key"
value = "second-value"
`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, Git{
		BinPath:          "/my/git/path",
		CatfileCacheSize: 50,
		Config: []GitConfig{
			{Key: "first.key", Value: "first-value"},
			{Key: "second.key", Value: "second-value"},
		},
	}, cfg.Git)
}

func TestValidateGitConfig(t *testing.T) {
	testCases := []struct {
		desc        string
		configPairs []GitConfig
		expectedErr error
	}{
		{
			desc: "empty config is valid",
		},
		{
			desc: "valid config entry",
			configPairs: []GitConfig{
				{Key: "foo.bar", Value: "value"},
			},
		},
		{
			desc: "missing key",
			configPairs: []GitConfig{
				{Value: "value"},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"git", "config"},
					Message: "invalid configuration key '': key cannot be empty",
				},
			},
		},
		{
			desc: "key has no section",
			configPairs: []GitConfig{
				{Key: "foo", Value: "value"},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"git", "config"},
					Message: "invalid configuration key 'foo': key must contain at least one section",
				},
			},
		},
		{
			desc: "key with leading dot",
			configPairs: []GitConfig{
				{Key: ".foo.bar", Value: "value"},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"git", "config"},
					Message: "invalid configuration key '.foo.bar': key must not start or end with a dot",
				},
			},
		},
		{
			desc: "key with trailing dot",
			configPairs: []GitConfig{
				{Key: "foo.bar.", Value: "value"},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"git", "config"},
					Message: "invalid configuration key 'foo.bar.': key must not start or end with a dot",
				},
			},
		},
		{
			desc: "key has assignment",
			configPairs: []GitConfig{
				{Key: "foo.bar=value", Value: "value"},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"git", "config"},
					Message: "invalid configuration key 'foo.bar=value': key cannot contain assignment",
				},
			},
		},
		{
			desc: "multiple problems",
			configPairs: []GitConfig{
				{Key: "foo", Value: "value"},
				{Key: "foo.bar=value", Value: "value"},
			},
			expectedErr: ValidationErrors{
				{
					Key:     []string{"git", "config"},
					Message: "invalid configuration key 'foo': key must contain at least one section",
				},
				{
					Key:     []string{"git", "config"},
					Message: "invalid configuration key 'foo.bar=value': key cannot contain assignment",
				},
			},
		},
		{
			desc: "missing value",
			configPairs: []GitConfig{
				{Key: "foo.bar", Value: ""},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{BinDir: testhelper.TempDir(t), Git: Git{Config: tc.configPairs}}
			require.Equal(t, tc.expectedErr, cfg.validateGit())
		})
	}
}

func TestValidateShellPath(t *testing.T) {
	tmpDir := testhelper.TempDir(t)

	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "bin"), perm.SharedDir))
	tmpFile := filepath.Join(tmpDir, "my-file")
	require.NoError(t, os.WriteFile(tmpFile, []byte{}, perm.PublicFile))

	testCases := []struct {
		desc        string
		path        string
		expectedErr error
	}{
		{
			desc: "When no Shell Path set",
			path: "",
			expectedErr: ValidationErrors{
				{
					Key:     []string{"gitlab-shell", "dir"},
					Message: "is not set",
				},
			},
		},
		{
			desc: "When Shell Path set to non-existing path",
			path: "/non/existing/path",
			expectedErr: ValidationErrors{
				{
					Key:     []string{"gitlab-shell", "dir"},
					Message: "'/non/existing/path' dir doesn't exist",
				},
			},
		},
		{
			desc: "When Shell Path set to non-dir path",
			path: tmpFile,
			expectedErr: ValidationErrors{
				{
					Key:     []string{"gitlab-shell", "dir"},
					Message: fmt.Sprintf("'%s' is not a dir", tmpFile),
				},
			},
		},
		{
			desc: "When Shell Path set to a valid directory",
			path: tmpDir,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{GitlabShell: GitlabShell{Dir: tc.path}}
			err := cfg.validateShell()
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestValidateListeners(t *testing.T) {
	testCases := []struct {
		desc string
		Cfg
		expectedErr error
	}{
		{
			desc: "empty",
			expectedErr: ValidationErrors{
				{
					Key:     []string{"socket_path"},
					Message: `none of 'socket_path', 'listen_addr' or 'tls_listen_addr' is set`,
				},
				{
					Key:     []string{"listen_addr"},
					Message: `none of 'socket_path', 'listen_addr' or 'tls_listen_addr' is set`,
				},
				{
					Key:     []string{"tls_listen_addr"},
					Message: `none of 'socket_path', 'listen_addr' or 'tls_listen_addr' is set`,
				},
			},
		},
		{desc: "socket only", Cfg: Cfg{SocketPath: "/foo/bar"}},
		{desc: "tcp only", Cfg: Cfg{ListenAddr: "a.b.c.d:1234"}},
		{desc: "tls only", Cfg: Cfg{TLSListenAddr: "a.b.c.d:1234"}},
		{desc: "both socket and tcp", Cfg: Cfg{SocketPath: "/foo/bar", ListenAddr: "a.b.c.d:1234"}},
		{desc: "all addresses", Cfg: Cfg{SocketPath: "/foo/bar", ListenAddr: "a.b.c.d:1234", TLSListenAddr: "a.b.c.d:1234"}},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.Cfg.validateListeners()
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestLoadGracefulRestartTimeout(t *testing.T) {
	tests := []struct {
		name     string
		config   string
		expected duration.Duration
	}{
		{
			name:     "default value",
			expected: duration.Duration(1 * time.Minute),
		},
		{
			name:     "8m03s",
			config:   `graceful_restart_timeout = "8m03s"`,
			expected: duration.Duration(8*time.Minute + 3*time.Second),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpFile := strings.NewReader(test.config)

			cfg, err := Load(tmpFile)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, cfg.GracefulRestartTimeout)
		})
	}
}

func TestGitlabShellDefaults(t *testing.T) {
	gitlabShellDir := "/dir"

	tmpFile := strings.NewReader(fmt.Sprintf(`[gitlab-shell]
dir = '%s'`, gitlabShellDir))
	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, Gitlab{
		SecretFile: filepath.Join(gitlabShellDir, ".gitlab_shell_secret"),
	}, cfg.Gitlab)
	require.Equal(t, Hooks{
		CustomHooksDir: filepath.Join(gitlabShellDir, "hooks"),
	}, cfg.Hooks)
}

func TestSetupRuntimeDirectory_validateInternalSocket(t *testing.T) {
	verifyPathDoesNotExist := func(t *testing.T, runtimeDir string, actualErr error) {
		require.EqualError(t, actualErr, fmt.Sprintf("creating runtime directory: mkdir %s/gitaly-%d: no such file or directory", runtimeDir, os.Getpid()))
	}

	testCases := []struct {
		desc   string
		setup  func(t *testing.T) string
		verify func(t *testing.T, runtimeDir string, actualErr error)
	}{
		{
			desc: "non existing directory",
			setup: func(t *testing.T) string {
				return "/path/does/not/exist"
			},
			verify: verifyPathDoesNotExist,
		},
		{
			desc: "symlinked runtime directory",
			setup: func(t *testing.T) string {
				runtimeDir := testhelper.TempDir(t)
				require.NoError(t, os.Mkdir(filepath.Join(runtimeDir, "sock.d"), perm.PublicDir))

				// Create a symlink which points to the real runtime directory.
				symlinkDir := testhelper.TempDir(t)
				symlink := filepath.Join(symlinkDir, "symlink-to-runtime-dir")
				require.NoError(t, os.Symlink(runtimeDir, symlink))

				return symlink
			},
		},
		{
			desc: "broken symlinked runtime directory",
			setup: func(t *testing.T) string {
				symlinkDir := testhelper.TempDir(t)
				symlink := filepath.Join(symlinkDir, "symlink-to-runtime-dir")
				require.NoError(t, os.Symlink("/path/does/not/exist", symlink))
				return symlink
			},
			verify: verifyPathDoesNotExist,
		},
		{
			desc: "socket can't be created",
			setup: func(t *testing.T) string {
				tempDir := testhelper.TempDir(t)

				runtimeDirTooLongForSockets := filepath.Join(tempDir, strings.Repeat("/nested_directory", 10))
				socketDir := filepath.Join(runtimeDirTooLongForSockets, "sock.d")
				require.NoError(t, os.MkdirAll(socketDir, perm.PublicDir))

				return runtimeDirTooLongForSockets
			},
			verify: func(t *testing.T, runtimeDir string, actualErr error) {
				require.EqualError(t, actualErr, "failed creating internal test socket: invalid argument: your socket path is likely too long, please change Gitaly's runtime directory")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			runtimeDir := tc.setup(t)

			cfg := Cfg{
				RuntimeDir: runtimeDir,
			}

			_, actualErr := SetupRuntimeDirectory(cfg, os.Getpid())
			if tc.verify == nil {
				require.NoError(t, actualErr)
			} else {
				tc.verify(t, cfg.RuntimeDir, actualErr)
			}
		})
	}
}

func TestLoadDailyMaintenance(t *testing.T) {
	for _, tt := range []struct {
		name        string
		rawCfg      string
		expect      DailyJob
		loadErr     error
		validateErr error
	}{
		{
			name: "success",
			rawCfg: `[[storage]]
			name = "default"
			path = "/"

			[daily_maintenance]
			start_hour = 11
			start_minute = 23
			duration = "45m"
			storages = ["default"]
			`,
			expect: DailyJob{
				Hour:     11,
				Minute:   23,
				Duration: duration.Duration(45 * time.Minute),
				Storages: []string{"default"},
			},
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 24`,
			expect: DailyJob{
				Hour: 24,
			},
			validateErr: errors.New("daily maintenance specified hour '24' outside range (0-23)"),
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 60`,
			expect: DailyJob{
				Hour: 60,
			},
			validateErr: errors.New("daily maintenance specified hour '60' outside range (0-23)"),
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 0
			start_minute = 61`,
			expect: DailyJob{
				Hour:   0,
				Minute: 61,
			},
			validateErr: errors.New("daily maintenance specified minute '61' outside range (0-59)"),
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 0
			start_minute = 59
			duration = "86401s"`,
			expect: DailyJob{
				Hour:     0,
				Minute:   59,
				Duration: duration.Duration(24*time.Hour + time.Second),
			},
			validateErr: errors.New("daily maintenance specified duration 24h0m1s must be less than 24 hours"),
		},
		{
			rawCfg: `[daily_maintenance]
			duration = "meow"`,
			expect:  DailyJob{},
			loadErr: errors.New("load toml: toml: time: invalid duration \"meow\""),
		},
		{
			rawCfg: `[daily_maintenance]
			storages = ["default"]`,
			expect: DailyJob{
				Storages: []string{"default"},
			},
			validateErr: errors.New(`daily maintenance specified storage "default" does not exist in configuration`),
		},
		{
			name: "default window",
			rawCfg: `[[storage]]
			name = "default"
			path = "/"
			`,
			expect: DailyJob{
				Hour:     12,
				Minute:   0,
				Duration: duration.Duration(10 * time.Minute),
				Storages: []string{"default"},
			},
		},
		{
			name: "override default window",
			rawCfg: `[[storage]]
			name = "default"
			path = "/"
			[daily_maintenance]
			disabled = true
			`,
			expect: DailyJob{
				Disabled: true,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := strings.NewReader(tt.rawCfg)
			cfg, err := Load(tmpFile)
			if err != nil {
				require.Contains(t, err.Error(), tt.loadErr.Error())
			}
			require.Equal(t, tt.expect, cfg.DailyMaintenance)
			require.Equal(t, tt.validateErr, cfg.validateMaintenance())
		})
	}
}

func TestValidateCgroups(t *testing.T) {
	type testCase struct {
		name        string
		rawCfg      string
		expect      cgroups.Config
		validateErr error
	}

	t.Run("old format", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "enabled success",
				rawCfg: `[cgroups]
				count = 10
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				[cgroups.memory]
				enabled = true
				limit = 1024
				[cgroups.cpu]
				enabled = true
				shares = 512`,
				expect: cgroups.Config{
					Count:         10,
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					Memory: cgroups.Memory{
						Enabled: true,
						Limit:   1024,
					},
					CPU: cgroups.CPU{
						Enabled: true,
						Shares:  512,
					},
					Repositories: cgroups.Repositories{
						Count:       10,
						MemoryBytes: 1024,
						CPUShares:   512,
					},
				},
			},
			{
				name: "empty mount point",
				rawCfg: `[cgroups]
				count = 10
				mountpoint = ""
				hierarchy_root = "baz"
				`,
				expect: cgroups.Config{
					Count:         10,
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "baz",
					Repositories: cgroups.Repositories{
						Count: 10,
					},
				},
			},
			{
				name: "empty hierarchy_root",
				rawCfg: `[cgroups]
				count = 10
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = ""`,
				expect: cgroups.Config{
					Count:         10,
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					Repositories: cgroups.Repositories{
						Count: 10,
					},
				},
			},
			{
				name: "cpu shares - zero",
				rawCfg: `[cgroups]
				count = 10
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				[cgroups.memory]
				enabled = true
				limit = 1024
				[cgroups.cpu]
				enabled = true
				shares = 0`,
				expect: cgroups.Config{
					Count:         10,
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					Memory: cgroups.Memory{
						Enabled: true,
						Limit:   1024,
					},
					CPU: cgroups.CPU{
						Enabled: true,
						Shares:  0,
					},
					Repositories: cgroups.Repositories{
						Count:       10,
						MemoryBytes: 1024,
					},
				},
			},
			{
				name: "memory limit - zero",
				rawCfg: `[cgroups]
				count = 10
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				[cgroups.memory]
				enabled = true
				limit = 0
				[cgroups.cpu]
				enabled = true
				shares = 512
				`,
				expect: cgroups.Config{
					Count:         10,
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					Memory: cgroups.Memory{
						Enabled: true,
						Limit:   0,
					},
					CPU: cgroups.CPU{
						Enabled: true,
						Shares:  512,
					},
					Repositories: cgroups.Repositories{
						Count:     10,
						CPUShares: 512,
					},
				},
			},
			{
				name: "repositories - zero count",
				rawCfg: `[cgroups]
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				[cgroups.repositories]
				`,
				expect: cgroups.Config{
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
				},
			},
		}
		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				tmpFile := strings.NewReader(tt.rawCfg)
				cfg, err := Load(tmpFile)
				require.NoError(t, err)
				require.Equal(t, tt.expect, cfg.Cgroups)
				require.Equal(t, tt.validateErr, cfg.validateCgroups())
			})
		}
	})

	t.Run("new format", func(t *testing.T) {
		testCases := []testCase{
			{
				name: "enabled success",
				rawCfg: `[cgroups]
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				[cgroups.repositories]
				count = 10
				memory_bytes = 1024
				cpu_shares = 512
				`,
				expect: cgroups.Config{
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					Repositories: cgroups.Repositories{
						Count:       10,
						MemoryBytes: 1024,
						CPUShares:   512,
					},
				},
			},
			{
				name: "repositories count is zero",
				rawCfg: `[cgroups]
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				[cgroups.repositories]
				count = 0
				memory_bytes = 1024
				cpu_shares = 512
				`,
				expect: cgroups.Config{
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					Repositories: cgroups.Repositories{
						MemoryBytes: 1024,
						CPUShares:   512,
					},
				},
			},
			{
				name: "memory is zero",
				rawCfg: `[cgroups]
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				[cgroups.repositories]
				count = 10
				cpu_shares = 512`,
				expect: cgroups.Config{
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					Repositories: cgroups.Repositories{
						Count:     10,
						CPUShares: 512,
					},
				},
			},
			{
				name: "repositories memory exceeds parent",
				rawCfg: `[cgroups]
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				memory_bytes = 1073741824
				cpu_shares = 1024
				[cgroups.repositories]
				count = 10
				memory_bytes = 2147483648
				cpu_shares = 128
				`,
				expect: cgroups.Config{
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					MemoryBytes:   1073741824,
					CPUShares:     1024,
					Repositories: cgroups.Repositories{
						Count:       10,
						MemoryBytes: 2147483648,
						CPUShares:   128,
					},
				},
				validateErr: errors.New("cgroups.repositories: memory limit cannot exceed parent"),
			},
			{
				name: "repositories cpu exceeds parent",
				rawCfg: `[cgroups]
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				memory_bytes = 1073741824
				cpu_shares = 128
				[cgroups.repositories]
				count = 10
				memory_bytes = 1024
				cpu_shares = 512
				`,
				expect: cgroups.Config{
					Mountpoint:    "/sys/fs/cgroup",
					HierarchyRoot: "gitaly",
					MemoryBytes:   1073741824,
					CPUShares:     128,
					Repositories: cgroups.Repositories{
						Count:       10,
						MemoryBytes: 1024,
						CPUShares:   512,
					},
				},
				validateErr: errors.New("cgroups.repositories: cpu shares cannot exceed parent"),
			},
			{
				name: "metrics enabled",
				rawCfg: `[cgroups]
				mountpoint = "/sys/fs/cgroup"
				hierarchy_root = "gitaly"
				metrics_enabled = true
				[cgroups.repositories]
				count = 10
				memory_bytes = 1024
				cpu_shares = 512
				`,
				expect: cgroups.Config{
					Mountpoint:     "/sys/fs/cgroup",
					HierarchyRoot:  "gitaly",
					MetricsEnabled: true,
					Repositories: cgroups.Repositories{
						Count:       10,
						MemoryBytes: 1024,
						CPUShares:   512,
					},
				},
			},
		}
		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				tmpFile := strings.NewReader(tt.rawCfg)
				cfg, err := Load(tmpFile)
				require.NoError(t, err)
				require.Equal(t, tt.expect, cfg.Cgroups)
				require.Equal(t, tt.validateErr, cfg.validateCgroups())
			})
		}
	})
}

func TestConfigurePackObjectsCache(t *testing.T) {
	storageConfig := `[[storage]]
name="default"
path="/foobar"
`

	testCases := []struct {
		desc string
		in   string
		out  StreamCacheConfig
		err  error
	}{
		{desc: "empty"},
		{
			desc: "enabled",
			in: storageConfig + `[pack_objects_cache]
enabled = true
`,
			out: StreamCacheConfig{Enabled: true, MaxAge: duration.Duration(5 * time.Minute), Dir: "/foobar/+gitaly/PackObjectsCache"},
		},
		{
			desc: "enabled with custom values",
			in: storageConfig + `[pack_objects_cache]
enabled = true
dir = "/bazqux"
max_age = "10m"
`,
			out: StreamCacheConfig{Enabled: true, MaxAge: duration.Duration(10 * time.Minute), Dir: "/bazqux"},
		},
		{
			desc: "enabled with 0 storages",
			in: `[pack_objects_cache]
enabled = true
`,
			err: errPackObjectsCacheNoStorages,
		},
		{
			desc: "enabled with negative max age",
			in: `[pack_objects_cache]
enabled = true
max_age = "-5m"
`,
			err: errPackObjectsCacheNegativeMaxAge,
		},
		{
			desc: "enabled with relative path",
			in: `[pack_objects_cache]
enabled = true
dir = "foobar"
`,
			err: errPackObjectsCacheRelativePath,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, err := Load(strings.NewReader(tc.in))
			require.NoError(t, err)

			err = cfg.configurePackObjectsCache()
			if tc.err != nil {
				require.Equal(t, tc.err, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.out, cfg.PackObjectsCache)
		})
	}
}

func TestValidateToken(t *testing.T) {
	require.NoError(t, (&Cfg{Auth: auth.Config{}}).validateToken())
	require.NoError(t, (&Cfg{Auth: auth.Config{Token: ""}}).validateToken())
	require.NoError(t, (&Cfg{Auth: auth.Config{Token: "secret"}}).validateToken())
	require.NoError(t, (&Cfg{Auth: auth.Config{Transitioning: true, Token: "secret"}}).validateToken())
}

func TestValidateBinDir(t *testing.T) {
	tmpDir := testhelper.TempDir(t)
	tmpFile := filepath.Join(tmpDir, "file")
	fp, err := os.Create(tmpFile)
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	for _, tc := range []struct {
		desc        string
		binDir      string
		expectedErr error
	}{
		{
			desc:   "ok",
			binDir: tmpDir,
		},
		{
			desc:   "empty",
			binDir: "",
			expectedErr: ValidationErrors{{
				Key:     []string{"bin_dir"},
				Message: "is not set",
			}},
		},
		{
			desc:   "path doesn't exist",
			binDir: "/not/exists",
			expectedErr: ValidationErrors{{
				Key:     []string{"bin_dir"},
				Message: "'/not/exists' dir doesn't exist",
			}},
		},
		{
			desc:   "is not a directory",
			binDir: tmpFile,
			expectedErr: ValidationErrors{{
				Key:     []string{"bin_dir"},
				Message: fmt.Sprintf(`'%s' is not a dir`, tmpFile),
			}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			errs := (&Cfg{BinDir: tc.binDir}).validateBinDir()
			require.Equal(t, tc.expectedErr, errs)
		})
	}
}

func TestSetupRuntimeDirectory(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		t.Run("empty runtime directory", func(t *testing.T) {
			cfg := Cfg{}
			runtimeDir, err := SetupRuntimeDirectory(cfg, os.Getpid())
			require.NoError(t, err)

			require.DirExists(t, runtimeDir)
			require.True(t, strings.HasPrefix(runtimeDir, filepath.Join(os.TempDir(), "gitaly-")))
		})

		t.Run("non-existent runtime directory", func(t *testing.T) {
			cfg := Cfg{
				RuntimeDir: "/does/not/exist",
			}

			_, err := SetupRuntimeDirectory(cfg, os.Getpid())
			require.EqualError(t, err, fmt.Sprintf("creating runtime directory: mkdir /does/not/exist/gitaly-%d: no such file or directory", os.Getpid()))
		})

		t.Run("existent runtime directory", func(t *testing.T) {
			dir := testhelper.TempDir(t)
			cfg := Cfg{
				RuntimeDir: dir,
			}

			runtimeDir, err := SetupRuntimeDirectory(cfg, os.Getpid())
			require.NoError(t, err)

			require.Equal(t, filepath.Join(dir, fmt.Sprintf("gitaly-%d", os.Getpid())), runtimeDir)
			require.DirExists(t, runtimeDir)
		})
	})

	t.Run("validation", func(t *testing.T) {
		dirPath := testhelper.TempDir(t)
		filePath := filepath.Join(dirPath, "file")
		require.NoError(t, os.WriteFile(filePath, nil, perm.SharedFile))

		for _, tc := range []struct {
			desc        string
			runtimeDir  string
			expectedErr error
		}{
			{
				desc:       "valid runtime directory",
				runtimeDir: dirPath,
			},
			{
				desc:       "unset",
				runtimeDir: "",
			},
			{
				desc:       "path doesn't exist",
				runtimeDir: "/does/not/exist",
				expectedErr: ValidationErrors{{
					Key:     []string{"runtime_dir"},
					Message: fmt.Sprintf("'%s' dir doesn't exist", "/does/not/exist"),
				}},
			},
			{
				desc:       "path is not a directory",
				runtimeDir: filePath,
				expectedErr: ValidationErrors{{
					Key:     []string{"runtime_dir"},
					Message: fmt.Sprintf(`'%s' is not a dir`, filePath),
				}},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				err := (&Cfg{RuntimeDir: tc.runtimeDir}).validateRuntimeDir()
				require.Equal(t, tc.expectedErr, err)
			})
		}
	})
}

func TestPackObjectsLimiting(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc              string
		rawCfg            string
		expectedErrString string
		expectedCfg       PackObjectsLimiting
	}{
		{
			desc: "using repo as key",
			rawCfg: `[pack_objects_limiting]
			key = "repository"
			max_concurrency = 20
			max_queue_wait = "10s"
			`,
			expectedCfg: PackObjectsLimiting{
				Key:            PackObjectsLimitingKeyRepository,
				MaxConcurrency: 20,
				MaxQueueWait:   duration.Duration(10 * time.Second),
			},
		},
		{
			desc: "using user as key",
			rawCfg: `[pack_objects_limiting]
			key = "user"
			max_concurrency = 10
			max_queue_wait = "1m"
			`,
			expectedCfg: PackObjectsLimiting{
				Key:            PackObjectsLimitingKeyUser,
				MaxConcurrency: 10,
				MaxQueueWait:   duration.Duration(1 * time.Minute),
			},
		},
		{
			desc: "invalid key",
			rawCfg: `[pack_objects_limiting]
			key = "project"
			max_concurrency = 10
			max_queue_wait = "1m"
			`,
			expectedErrString: "unsupported pack objects limiting key: project",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tmpFile := strings.NewReader(tc.rawCfg)
			cfg, err := Load(tmpFile)
			if tc.expectedErrString != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrString)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedCfg, cfg.PackObjectsLimiting)
		})
	}
}

func TestValidationErrors_Error(t *testing.T) {
	t.Parallel()

	t.Run("serialized", func(t *testing.T) {
		require.Equal(t, "1.2: msg1\n1: msg2",
			ValidationErrors{
				{Key: []string{"1", "2"}, Message: "msg1"},
				{Key: []string{"1"}, Message: "msg2"},
			}.Error(),
		)
	})

	t.Run("nothing to present", func(t *testing.T) {
		require.Equal(t, "", ValidationErrors{}.Error())
	})
}
