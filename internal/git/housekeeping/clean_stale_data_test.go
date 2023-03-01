package housekeeping

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"google.golang.org/grpc/peer"
)

type entryFinalState int

const (
	Delete entryFinalState = iota
	Keep
)

type entry interface {
	create(t *testing.T, parent string)
	validate(t *testing.T, parent string)
}

// fileEntry is an entry implementation for a file
type fileEntry struct {
	name       string
	mode       os.FileMode
	age        time.Duration
	finalState entryFinalState
}

func (f *fileEntry) create(t *testing.T, parent string) {
	t.Helper()

	filename := filepath.Join(parent, f.name)
	ff, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, perm.PrivateFile)
	assert.NoError(t, err, "file creation failed: %v", filename)
	err = ff.Close()
	assert.NoError(t, err, "file close failed: %v", filename)

	f.chmod(t, filename)
	f.chtimes(t, filename)
}

func (f *fileEntry) validate(t *testing.T, parent string) {
	t.Helper()

	filename := filepath.Join(parent, f.name)
	f.checkExistence(t, filename)
}

func (f *fileEntry) chmod(t *testing.T, filename string) {
	t.Helper()

	err := os.Chmod(filename, f.mode)
	assert.NoError(t, err, "chmod failed")
}

func (f *fileEntry) chtimes(t *testing.T, filename string) {
	t.Helper()

	filetime := time.Now().Add(-f.age)
	err := os.Chtimes(filename, filetime, filetime)
	assert.NoError(t, err, "chtimes failed")
}

func (f *fileEntry) checkExistence(t *testing.T, filename string) {
	t.Helper()
	_, err := os.Stat(filename)
	if err == nil && f.finalState == Delete {
		t.Errorf("Expected %v to have been deleted.", filename)
	} else if err != nil && f.finalState == Keep {
		t.Errorf("Expected %v to not have been deleted.", filename)
	}
}

// dirEntry is an entry implementation for a directory. A file with entries
type dirEntry struct {
	fileEntry
	entries []entry
}

func (d *dirEntry) create(t *testing.T, parent string) {
	t.Helper()

	dirname := filepath.Join(parent, d.name)

	if err := os.Mkdir(dirname, perm.PrivateDir); err != nil {
		require.True(t, os.IsExist(err), "mkdir failed: %v", dirname)
	}

	for _, e := range d.entries {
		e.create(t, dirname)
	}

	// Apply permissions and times after the children have been created
	d.chmod(t, dirname)
	d.chtimes(t, dirname)
}

func (d *dirEntry) validate(t *testing.T, parent string) {
	t.Helper()

	dirname := filepath.Join(parent, d.name)
	d.checkExistence(t, dirname)

	for _, e := range d.entries {
		e.validate(t, dirname)
	}
}

func f(name string, mode os.FileMode, age time.Duration, finalState entryFinalState) *fileEntry {
	return &fileEntry{name, mode, age, finalState}
}

func d(name string, mode os.FileMode, age time.Duration, finalState entryFinalState, entries ...entry) *dirEntry {
	return &dirEntry{fileEntry{name, mode, age, finalState}, entries}
}

type cleanStaleDataMetrics struct {
	configkeys     int
	configsections int
	objects        int
	locks          int
	refs           int
	reflocks       int
	refsEmptyDir   int
	packedRefsLock int
	packedRefsNew  int
	serverInfo     int
}

func requireCleanStaleDataMetrics(t *testing.T, m *RepositoryManager, metrics cleanStaleDataMetrics) {
	t.Helper()

	var builder strings.Builder

	_, err := builder.WriteString("# HELP gitaly_housekeeping_pruned_files_total Total number of files pruned\n")
	require.NoError(t, err)
	_, err = builder.WriteString("# TYPE gitaly_housekeeping_pruned_files_total counter\n")
	require.NoError(t, err)

	for metric, expectedValue := range map[string]int{
		"configkeys":     metrics.configkeys,
		"configsections": metrics.configsections,
		"objects":        metrics.objects,
		"locks":          metrics.locks,
		"refs":           metrics.refs,
		"reflocks":       metrics.reflocks,
		"packedrefslock": metrics.packedRefsLock,
		"packedrefsnew":  metrics.packedRefsNew,
		"refsemptydir":   metrics.refsEmptyDir,
		"serverinfo":     metrics.serverInfo,
	} {
		_, err := builder.WriteString(fmt.Sprintf("gitaly_housekeeping_pruned_files_total{filetype=%q} %d\n", metric, expectedValue))
		require.NoError(t, err)
	}

	require.NoError(t, testutil.CollectAndCompare(m, strings.NewReader(builder.String()), "gitaly_housekeeping_pruned_files_total"))
}

func TestRepositoryManager_CleanStaleData(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name            string
		entries         []entry
		expectedMetrics cleanStaleDataMetrics
	}{
		{
			name: "clean",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					f("a", perm.PrivateFile, 24*time.Hour, Keep),
					f("b", perm.PrivateFile, 24*time.Hour, Keep),
					f("c", perm.PrivateFile, 24*time.Hour, Keep),
				),
			},
		},
		{
			name: "emptyperms",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					f("b", perm.PrivateFile, 24*time.Hour, Keep),
					f("tmp_a", 0o000, 2*time.Hour, Keep),
				),
			},
		},
		{
			name: "emptytempdir",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					d("tmp_d", 0o000, 240*time.Hour, Keep),
					f("b", perm.PrivateFile, 24*time.Hour, Keep),
				),
			},
		},
		{
			name: "oldtempfile",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					f("tmp_a", perm.SharedFile, 240*time.Hour, Delete),
					f("b", perm.PrivateFile, 24*time.Hour, Keep),
				),
			},
			expectedMetrics: cleanStaleDataMetrics{
				objects: 1,
			},
		},
		{
			name: "subdir temp file",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					d("a", perm.GroupPrivateDir, 240*time.Hour, Keep,
						f("tmp_b", perm.PrivateFile, 240*time.Hour, Delete),
					),
				),
			},
			expectedMetrics: cleanStaleDataMetrics{
				objects: 1,
			},
		},
		{
			name: "inaccessible tmp directory",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					d("tmp_a", 0o000, 240*time.Hour, Keep,
						f("tmp_b", perm.PrivateFile, 240*time.Hour, Delete),
					),
				),
			},
		},
		{
			name: "deeply nested inaccessible tmp directory",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					d("tmp_a", perm.PrivateDir, 240*time.Hour, Keep,
						d("tmp_a", perm.PrivateDir, 24*time.Hour, Keep,
							f("tmp_b", 0o000, 240*time.Hour, Delete),
						),
					),
				),
			},
			expectedMetrics: cleanStaleDataMetrics{
				objects: 1,
			},
		},
		{
			name: "files outside of object database",
			entries: []entry{
				f("tmp_a", perm.SharedFile, 240*time.Hour, Keep),
				d("info", perm.PrivateDir, 240*time.Hour, Keep,
					f("tmp_a", perm.SharedFile, 240*time.Hour, Keep),
				),
			},
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			// We need to fix permissions so we don't fail to
			// remove the temporary directory after the test.
			defer func() {
				require.NoError(t, FixDirectoryPermissions(ctx, repoPath))
			}()

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			mgr := NewManager(cfg.Prometheus, nil)

			require.NoError(t, mgr.CleanStaleData(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}

			requireCleanStaleDataMetrics(t, mgr, tc.expectedMetrics)
		})
	}
}

func TestRepositoryManager_CleanStaleData_references(t *testing.T) {
	t.Parallel()
	type ref struct {
		name string
		age  time.Duration
		size int
	}

	testcases := []struct {
		desc            string
		refs            []ref
		expected        []string
		expectedMetrics cleanStaleDataMetrics
	}{
		{
			desc: "normal reference",
			refs: []ref{
				{name: "refs/heads/master", age: 1 * time.Second, size: 40},
			},
			expected: []string{
				"refs/heads/master",
			},
		},
		{
			desc: "recent empty reference is not deleted",
			refs: []ref{
				{name: "refs/heads/master", age: 1 * time.Hour, size: 0},
			},
			expected: []string{
				"refs/heads/master",
			},
		},
		{
			desc: "old empty reference is deleted",
			refs: []ref{
				{name: "refs/heads/master", age: 25 * time.Hour, size: 0},
			},
			expected: nil,
			expectedMetrics: cleanStaleDataMetrics{
				refs: 1,
			},
		},
		{
			desc: "multiple references",
			refs: []ref{
				{name: "refs/keep/kept-because-recent", age: 1 * time.Hour, size: 0},
				{name: "refs/keep/kept-because-nonempty", age: 25 * time.Hour, size: 1},
				{name: "refs/keep/prune", age: 25 * time.Hour, size: 0},
				{name: "refs/tags/kept-because-recent", age: 1 * time.Hour, size: 0},
				{name: "refs/tags/kept-because-nonempty", age: 25 * time.Hour, size: 1},
				{name: "refs/tags/prune", age: 25 * time.Hour, size: 0},
				{name: "refs/heads/kept-because-recent", age: 1 * time.Hour, size: 0},
				{name: "refs/heads/kept-because-nonempty", age: 25 * time.Hour, size: 1},
				{name: "refs/heads/prune", age: 25 * time.Hour, size: 0},
			},
			expected: []string{
				"refs/keep/kept-because-recent",
				"refs/keep/kept-because-nonempty",
				"refs/tags/kept-because-recent",
				"refs/tags/kept-because-nonempty",
				"refs/heads/kept-because-recent",
				"refs/heads/kept-because-nonempty",
			},
			expectedMetrics: cleanStaleDataMetrics{
				refs: 3,
			},
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, ref := range tc.refs {
				path := filepath.Join(repoPath, ref.name)

				require.NoError(t, os.MkdirAll(filepath.Dir(path), perm.SharedDir))
				require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0}, ref.size), perm.SharedFile))
				filetime := time.Now().Add(-ref.age)
				require.NoError(t, os.Chtimes(path, filetime, filetime))
			}

			mgr := NewManager(cfg.Prometheus, nil)

			require.NoError(t, mgr.CleanStaleData(ctx, repo))

			var actual []string
			require.NoError(t, filepath.Walk(filepath.Join(repoPath, "refs"), func(path string, info os.FileInfo, _ error) error {
				if !info.IsDir() {
					ref, err := filepath.Rel(repoPath, path)
					require.NoError(t, err)
					actual = append(actual, ref)
				}
				return nil
			}))

			require.ElementsMatch(t, tc.expected, actual)

			requireCleanStaleDataMetrics(t, mgr, tc.expectedMetrics)
		})
	}
}

func TestRepositoryManager_CleanStaleData_emptyRefDirs(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name            string
		entries         []entry
		expectedMetrics cleanStaleDataMetrics
	}{
		{
			name: "unrelated empty directories",
			entries: []entry{
				d("objects", perm.PrivateDir, 240*time.Hour, Keep,
					d("empty", perm.PrivateDir, 240*time.Hour, Keep),
				),
			},
		},
		{
			name: "empty ref dir gets retained",
			entries: []entry{
				d("refs", perm.PrivateDir, 240*time.Hour, Keep),
			},
		},
		{
			name: "empty nested non-stale ref dir gets kept",
			entries: []entry{
				d("refs", perm.PrivateDir, 240*time.Hour, Keep,
					d("nested", perm.PrivateDir, 23*time.Hour, Keep),
				),
			},
		},
		{
			name: "empty nested stale ref dir gets pruned",
			entries: []entry{
				d("refs", perm.PrivateDir, 240*time.Hour, Keep,
					d("nested", perm.PrivateDir, 240*time.Hour, Delete),
				),
			},
			expectedMetrics: cleanStaleDataMetrics{
				refsEmptyDir: 1,
			},
		},
		{
			name: "hierarchy of nested stale ref dirs gets pruned",
			entries: []entry{
				d("refs", perm.PrivateDir, 240*time.Hour, Keep,
					d("first", perm.PrivateDir, 240*time.Hour, Delete,
						d("second", perm.PrivateDir, 240*time.Hour, Delete),
					),
				),
			},
			expectedMetrics: cleanStaleDataMetrics{
				refsEmptyDir: 2,
			},
		},
		{
			name: "hierarchy with intermediate non-stale ref dir gets kept",
			entries: []entry{
				d("refs", perm.PrivateDir, 240*time.Hour, Keep,
					d("first", perm.PrivateDir, 240*time.Hour, Keep,
						d("second", perm.PrivateDir, 1*time.Hour, Keep,
							d("third", perm.PrivateDir, 24*time.Hour, Delete),
						),
					),
				),
			},
			expectedMetrics: cleanStaleDataMetrics{
				refsEmptyDir: 1,
			},
		},
		{
			name: "stale hierrachy with refs gets partially retained",
			entries: []entry{
				d("refs", perm.PrivateDir, 240*time.Hour, Keep,
					d("first", perm.PrivateDir, 240*time.Hour, Keep,
						d("second", perm.PrivateDir, 240*time.Hour, Delete,
							d("third", perm.PrivateDir, 24*time.Hour, Delete),
						),
						d("other", perm.PrivateDir, 240*time.Hour, Keep,
							f("ref", perm.PrivateFile, 1*time.Hour, Keep),
						),
					),
				),
			},
			expectedMetrics: cleanStaleDataMetrics{
				refsEmptyDir: 2,
			},
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			mgr := NewManager(cfg.Prometheus, nil)

			require.NoError(t, mgr.CleanStaleData(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}

			requireCleanStaleDataMetrics(t, mgr, tc.expectedMetrics)
		})
	}
}

func TestRepositoryManager_CleanStaleData_withSpecificFile(t *testing.T) {
	t.Parallel()

	entryInSubdir := func(e entry, subdirs ...string) entry {
		if len(subdirs) == 0 {
			return e
		}

		var topLevelDir, currentDir *dirEntry
		for _, subdir := range subdirs {
			dir := d(subdir, perm.PrivateDir, 1*time.Hour, Keep)
			if topLevelDir == nil {
				topLevelDir = dir
			}

			if currentDir != nil {
				currentDir.entries = []entry{dir}
			}

			currentDir = dir
		}

		currentDir.entries = []entry{e}

		return topLevelDir
	}

	for _, tc := range []struct {
		desc            string
		file            string
		subdirs         []string
		finder          staleFileFinderFn
		expectedMetrics cleanStaleDataMetrics
	}{
		{
			desc:   "locked HEAD",
			file:   "HEAD.lock",
			finder: findStaleLockfiles,
			expectedMetrics: cleanStaleDataMetrics{
				locks: 1,
			},
		},
		{
			desc:   "locked config",
			file:   "config.lock",
			finder: findStaleLockfiles,
			expectedMetrics: cleanStaleDataMetrics{
				locks: 1,
			},
		},
		{
			desc: "locked attributes",
			file: "attributes.lock",
			subdirs: []string{
				"info",
			},
			finder: findStaleLockfiles,
			expectedMetrics: cleanStaleDataMetrics{
				locks: 1,
			},
		},
		{
			desc: "locked alternates",
			file: "alternates.lock",
			subdirs: []string{
				"objects", "info",
			},
			finder: findStaleLockfiles,
			expectedMetrics: cleanStaleDataMetrics{
				locks: 1,
			},
		},
		{
			desc: "locked commit-graph-chain",
			file: "commit-graph-chain.lock",
			subdirs: []string{
				"objects", "info", "commit-graphs",
			},
			finder: findStaleLockfiles,
			expectedMetrics: cleanStaleDataMetrics{
				locks: 1,
			},
		},
		{
			desc:   "locked packed-refs",
			file:   "packed-refs.lock",
			finder: findPackedRefsLock,
			expectedMetrics: cleanStaleDataMetrics{
				packedRefsLock: 1,
			},
		},
		{
			desc:   "temporary packed-refs",
			file:   "packed-refs.new",
			finder: findPackedRefsNew,
			expectedMetrics: cleanStaleDataMetrics{
				packedRefsNew: 1,
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			mgr := NewManager(cfg.Prometheus, nil)

			require.NoError(t, mgr.CleanStaleData(ctx, repo))
			for _, subcase := range []struct {
				desc          string
				entry         entry
				expectedFiles []string
			}{
				{
					desc:  fmt.Sprintf("fresh %s is kept", tc.file),
					entry: f(tc.file, perm.PrivateFile, 10*time.Minute, Keep),
				},
				{
					desc: fmt.Sprintf("stale %s in subdir is kept", tc.file),
					entry: d("subdir", perm.PrivateDir, 240*time.Hour, Keep,
						f(tc.file, perm.PrivateFile, 24*time.Hour, Keep),
					),
				},
				{
					desc:  fmt.Sprintf("stale %s is deleted", tc.file),
					entry: f(tc.file, perm.PrivateFile, 61*time.Minute, Delete),
					expectedFiles: []string{
						filepath.Join(append([]string{repoPath}, append(tc.subdirs, tc.file)...)...),
					},
				},
				{
					desc:  fmt.Sprintf("%q is kept", tc.file[:len(tc.file)-1]),
					entry: f(tc.file[:len(tc.file)-1], perm.PrivateFile, 61*time.Minute, Keep),
				},
				{
					desc:  fmt.Sprintf("%q is kept", "~"+tc.file),
					entry: f("~"+tc.file, perm.PrivateFile, 61*time.Minute, Keep),
				},
				{
					desc:  fmt.Sprintf("%q is kept", tc.file+"~"),
					entry: f(tc.file+"~", perm.PrivateFile, 61*time.Minute, Keep),
				},
			} {
				t.Run(subcase.desc, func(t *testing.T) {
					entry := entryInSubdir(subcase.entry, tc.subdirs...)
					entry.create(t, repoPath)

					staleFiles, err := tc.finder(ctx, repoPath)
					require.NoError(t, err)
					require.ElementsMatch(t, subcase.expectedFiles, staleFiles)

					require.NoError(t, mgr.CleanStaleData(ctx, repo))

					entry.validate(t, repoPath)
				})
			}

			requireCleanStaleDataMetrics(t, mgr, tc.expectedMetrics)
		})
	}
}

func TestRepositoryManager_CleanStaleData_serverInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	entries := []entry{
		d("info", perm.SharedDir, 0, Keep,
			f("ref", perm.SharedFile, 0, Keep),
			f("refs", perm.SharedFile, 0, Delete),
			f("refsx", perm.SharedFile, 0, Keep),
			f("refs_123456", perm.SharedFile, 0, Delete),
		),
		d("objects", perm.SharedDir, 0, Keep,
			d("info", perm.SharedDir, 0, Keep,
				f("pack", perm.SharedFile, 0, Keep),
				f("packs", perm.SharedFile, 0, Delete),
				f("packsx", perm.SharedFile, 0, Keep),
				f("packs_123456", perm.SharedFile, 0, Delete),
			),
		),
	}

	for _, entry := range entries {
		entry.create(t, repoPath)
	}

	staleFiles, err := findServerInfo(ctx, repoPath)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		filepath.Join(repoPath, "info/refs"),
		filepath.Join(repoPath, "info/refs_123456"),
		filepath.Join(repoPath, "objects/info/packs"),
		filepath.Join(repoPath, "objects/info/packs_123456"),
	}, staleFiles)

	mgr := NewManager(cfg.Prometheus, nil)

	require.NoError(t, mgr.CleanStaleData(ctx, repo))

	for _, entry := range entries {
		entry.validate(t, repoPath)
	}

	requireCleanStaleDataMetrics(t, mgr, cleanStaleDataMetrics{
		serverInfo: 4,
	})
}

func TestRepositoryManager_CleanStaleData_referenceLocks(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc                   string
		entries                []entry
		expectedReferenceLocks []string
		expectedMetrics        cleanStaleDataMetrics
	}{
		{
			desc: "fresh lock is kept",
			entries: []entry{
				d("refs", perm.SharedDir, 0*time.Hour, Keep,
					f("main", perm.PublicFile, 10*time.Minute, Keep),
					f("main.lock", perm.PublicFile, 10*time.Minute, Keep),
				),
			},
		},
		{
			desc: "stale lock is deleted",
			entries: []entry{
				d("refs", perm.SharedDir, 0*time.Hour, Keep,
					f("main", perm.PublicFile, 1*time.Hour, Keep),
					f("main.lock", perm.PublicFile, 1*time.Hour, Delete),
				),
			},
			expectedReferenceLocks: []string{
				"refs/main.lock",
			},
			expectedMetrics: cleanStaleDataMetrics{
				reflocks: 1,
			},
		},
		{
			desc: "nested reference locks are deleted",
			entries: []entry{
				d("refs", perm.SharedDir, 0*time.Hour, Keep,
					d("tags", perm.SharedDir, 0*time.Hour, Keep,
						f("main", perm.PublicFile, 1*time.Hour, Keep),
						f("main.lock", perm.PublicFile, 1*time.Hour, Delete),
					),
					d("heads", perm.SharedDir, 0*time.Hour, Keep,
						f("main", perm.PublicFile, 1*time.Hour, Keep),
						f("main.lock", perm.PublicFile, 1*time.Hour, Delete),
					),
					d("foobar", perm.SharedDir, 0*time.Hour, Keep,
						f("main", perm.PublicFile, 1*time.Hour, Keep),
						f("main.lock", perm.PublicFile, 1*time.Hour, Delete),
					),
				),
			},
			expectedReferenceLocks: []string{
				"refs/tags/main.lock",
				"refs/heads/main.lock",
				"refs/foobar/main.lock",
			},
			expectedMetrics: cleanStaleDataMetrics{
				reflocks: 3,
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg := testcfg.Build(t)

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			// We need to recreate the temporary directory on each
			// run, so we don't have the full path available when
			// creating the testcases.
			var expectedReferenceLocks []string
			for _, referenceLock := range tc.expectedReferenceLocks {
				expectedReferenceLocks = append(expectedReferenceLocks, filepath.Join(repoPath, referenceLock))
			}

			staleLockfiles, err := findStaleReferenceLocks(ctx, repoPath)
			require.NoError(t, err)
			require.ElementsMatch(t, expectedReferenceLocks, staleLockfiles)

			mgr := NewManager(cfg.Prometheus, nil)

			require.NoError(t, mgr.CleanStaleData(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}

			requireCleanStaleDataMetrics(t, mgr, tc.expectedMetrics)
		})
	}
}

type mockDirEntry struct {
	fs.DirEntry
	isDir bool
	name  string
	fi    fs.FileInfo
}

func (m mockDirEntry) Name() string {
	return m.name
}

func (m mockDirEntry) IsDir() bool {
	return m.isDir
}

func (m mockDirEntry) Info() (fs.FileInfo, error) {
	return m.fi, nil
}

type mockFileInfo struct {
	fs.FileInfo
	modTime time.Time
}

func (m mockFileInfo) ModTime() time.Time {
	return m.modTime
}

func TestIsStaleTemporaryObject(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		dirEntry      fs.DirEntry
		expectIsStale bool
	}{
		{
			name: "regular_file",
			dirEntry: mockDirEntry{
				name: "objects",
				fi: mockFileInfo{
					modTime: time.Now().Add(-1 * time.Hour),
				},
			},
			expectIsStale: false,
		},
		{
			name: "directory",
			dirEntry: mockDirEntry{
				name:  "tmp",
				isDir: true,
				fi: mockFileInfo{
					modTime: time.Now().Add(-1 * time.Hour),
				},
			},
			expectIsStale: false,
		},
		{
			name: "recent_time_file",
			dirEntry: mockDirEntry{
				name: "tmp_DELETEME",
				fi: mockFileInfo{
					modTime: time.Now().Add(-1 * time.Hour),
				},
			},
			expectIsStale: false,
		},
		{
			name: "old temp file",
			dirEntry: mockDirEntry{
				name: "tmp_DELETEME",
				fi: mockFileInfo{
					modTime: time.Now().Add(-8 * 24 * time.Hour),
				},
			},
			expectIsStale: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isStale, err := isStaleTemporaryObject(tc.dirEntry)
			require.NoError(t, err)
			require.Equal(t, tc.expectIsStale, isStale)
		})
	}
}

func TestRepositoryManager_CleanStaleData_missingRepo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	require.NoError(t, os.RemoveAll(repoPath))

	require.NoError(t, NewManager(cfg.Prometheus, nil).CleanStaleData(ctx, repo))
}

func TestRepositoryManager_CleanStaleData_unsetConfiguration(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	configPath := filepath.Join(repoPath, "config")

	require.NoError(t, os.WriteFile(configPath, []byte(
		`[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
	commitGraph = true
	sparseCheckout = true
	splitIndex = false
[remote "first"]
	fetch = baz
	mirror = baz
	prune = baz
	url = baz
[http "first"]
	extraHeader = barfoo
[http "second"]
	extraHeader = barfoo
[http]
	extraHeader = untouched
[http "something"]
	else = untouched
[totally]
	unrelated = untouched
`), perm.SharedFile))

	mgr := NewManager(cfg.Prometheus, nil)

	require.NoError(t, mgr.CleanStaleData(ctx, repo))
	require.Equal(t,
		`[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[http]
	extraHeader = untouched
[http "something"]
	else = untouched
[totally]
	unrelated = untouched
`, string(testhelper.MustReadFile(t, configPath)))

	requireCleanStaleDataMetrics(t, mgr, cleanStaleDataMetrics{
		configkeys: 1,
	})
}

func TestRepositoryManager_CleanStaleData_unsetConfigurationTransactional(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.some.extraHeader", "value")

	txManager := transaction.NewTrackingManager()

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})

	require.NoError(t, NewManager(cfg.Prometheus, txManager).CleanStaleData(ctx, repo))
	require.Equal(t, 2, len(txManager.Votes()))

	configKeys := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local", "--name-only")

	expectedConfig := "core.repositoryformatversion\ncore.filemode\ncore.bare\n"

	if gittest.DefaultObjectHash.Format == "sha256" {
		expectedConfig = expectedConfig + "extensions.objectformat\n"
	}

	if runtime.GOOS == "darwin" {
		expectedConfig = expectedConfig + "core.ignorecase\ncore.precomposeunicode\n"
	}
	require.Equal(t, expectedConfig, string(configKeys))
}

func TestRepositoryManager_CleanStaleData_pruneEmptyConfigSections(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	configPath := filepath.Join(repoPath, "config")

	require.NoError(t, os.WriteFile(configPath, []byte(
		`[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[uploadpack]
	allowAnySHA1InWant = true
[remote "tmp-8be1695862b62390d1f873f9164122e4"]
[remote "tmp-d97f78c39fde4b55e0d0771dfc0501ef"]
[remote "tmp-23a2471e7084e1548ef47bbc9d6afff6"]
[remote "tmp-d76633a16d61f6681de396ec9ecfd7b5"]
	prune = true
[remote "tmp-8fbf8d5e7585d48668f1791284a912ef"]
[remote "tmp-f539c59068f291e52f1140e39830f9ca"]
[remote "tmp-17b67d28909768db3213917255c72af2"]
	prune = true
[remote "tmp-03b5e8c765135b343214d471843a062a"]
[remote "tmp-f57338181aca1d599669dbb71ce9ce57"]
[remote "tmp-8c948ca94832c2725733e48cb2902287"]
`), perm.SharedFile))

	mgr := NewManager(cfg.Prometheus, nil)

	require.NoError(t, mgr.CleanStaleData(ctx, repo))
	require.Equal(t, `[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[uploadpack]
	allowAnySHA1InWant = true
`, string(testhelper.MustReadFile(t, configPath)))

	requireCleanStaleDataMetrics(t, mgr, cleanStaleDataMetrics{
		configkeys:     1,
		configsections: 7,
	})
}

func TestPruneEmptyConfigSections(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	configPath := filepath.Join(repoPath, "config")

	for _, tc := range []struct {
		desc                    string
		configData              string
		expectedData            string
		expectedSkippedSections int
	}{
		{
			desc:         "empty",
			configData:   "",
			expectedData: "",
		},
		{
			desc:         "newline only",
			configData:   "\n",
			expectedData: "\n",
		},
		{
			desc:         "no stripping",
			configData:   "[foo]\nbar = baz\n",
			expectedData: "[foo]\nbar = baz\n",
		},
		{
			desc:         "no stripping with missing newline",
			configData:   "[foo]\nbar = baz",
			expectedData: "[foo]\nbar = baz",
		},
		{
			desc:         "multiple sections",
			configData:   "[foo]\nbar = baz\n[bar]\nfoo = baz\n",
			expectedData: "[foo]\nbar = baz\n[bar]\nfoo = baz\n",
		},
		{
			desc:         "missing newline",
			configData:   "[foo]\nbar = baz",
			expectedData: "[foo]\nbar = baz",
		},
		{
			desc:         "single comment",
			configData:   "# foobar\n",
			expectedData: "# foobar\n",
		},
		{
			// This is not correct, but we really don't want to start parsing
			// the config format completely. So we err on the side of caution
			// and just say this is fine.
			desc:                    "empty section with comment",
			configData:              "[foo]\n# comment\n[bar]\n[baz]\n",
			expectedData:            "[foo]\n# comment\n",
			expectedSkippedSections: 1,
		},
		{
			desc:         "empty section",
			configData:   "[foo]\n",
			expectedData: "",
		},
		{
			desc:                    "empty sections",
			configData:              "[foo]\n[bar]\n[baz]\n",
			expectedData:            "",
			expectedSkippedSections: 2,
		},
		{
			desc:                    "empty sections with missing newline",
			configData:              "[foo]\n[bar]\n[baz]",
			expectedData:            "",
			expectedSkippedSections: 2,
		},
		{
			desc:         "trailing empty section",
			configData:   "[foo]\nbar = baz\n[foo]\n",
			expectedData: "[foo]\nbar = baz\n",
		},
		{
			desc:                    "mixed keys and sections",
			configData:              "[empty]\n[nonempty]\nbar = baz\nbar = baz\n[empty]\n",
			expectedData:            "[nonempty]\nbar = baz\nbar = baz\n",
			expectedSkippedSections: 1,
		},
		{
			desc: "real world example",
			configData: `[core]
        repositoryformatversion = 0
        filemode = true
        bare = true
[uploadpack]
        allowAnySHA1InWant = true
[remote "tmp-8be1695862b62390d1f873f9164122e4"]
[remote "tmp-d97f78c39fde4b55e0d0771dfc0501ef"]
[remote "tmp-23a2471e7084e1548ef47bbc9d6afff6"]
[remote "tmp-6ef9759bb14db34ca67de4681f0a812a"]
[remote "tmp-992cb6a0ea428a511cc2de3cde051227"]
[remote "tmp-a720c2b6794fdbad50f36f0a4e9501ff"]
[remote "tmp-4b4f6d68031aa1288613f40b1a433278"]
[remote "tmp-fc12da796c907e8ea5faed134806acfb"]
[remote "tmp-49e1fbb6eccdb89059a7231eef785d03"]
[remote "tmp-e504bbbed5d828cd96b228abdef4b055"]
[remote "tmp-36e856371fdacb7b4909240ba6bc0b34"]
[remote "tmp-9a1bc23bb2200b9426340a5ba934f5ba"]
[remote "tmp-49ead30f732995498e0585b569917c31"]
[remote "tmp-8419f1e1445ccd6e1c60aa421573447c"]
[remote "tmp-f7a91ec9415f984d3747cf608b0a7e9c"]
        prune = true
[remote "tmp-ea77d1e5348d07d693aa2bf8a2c98637"]
[remote "tmp-3f190ab463b804612cb007487e0cbb4d"]`,
			expectedData: `[core]
        repositoryformatversion = 0
        filemode = true
        bare = true
[uploadpack]
        allowAnySHA1InWant = true
[remote "tmp-f7a91ec9415f984d3747cf608b0a7e9c"]
        prune = true
`,
			expectedSkippedSections: 15,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t, os.WriteFile(configPath, []byte(tc.configData), perm.SharedFile))

			skippedSections, err := pruneEmptyConfigSections(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSkippedSections, skippedSections)

			require.Equal(t, tc.expectedData, string(testhelper.MustReadFile(t, configPath)))
		})
	}
}
