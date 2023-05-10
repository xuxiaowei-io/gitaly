package trace2hooks_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2hooks"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestPackObjectsMetrics(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc      string
		setupRepo func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer)
		assert    func(*testing.T, logrus.Fields)
	}{
		{
			desc: "pack blobs",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				for i := 0; i <= 10; i++ {
					content := fmt.Sprintf("hello %d", i)
					input.WriteString(gittest.WriteBlob(t, cfg, repoPath, []byte(content)).String())
					input.WriteString("\n")
				}

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields logrus.Fields) {
				require.Equal(t, 11, statFields["pack_objects.written_object_count"])
			},
		},
		{
			desc: "pack commits",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				input.WriteString(gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("reachable"), gittest.WithBranch("reachable")).String())
				input.WriteString("\n")
				input.WriteString(gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unreachable")).String())
				input.WriteString("\n")

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields logrus.Fields) {
				require.Equal(t, 3, statFields["pack_objects.written_object_count"])
			},
		},
		{
			desc: "pack refs",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("commit"), gittest.WithBranch("main"))
				for i := 0; i <= 10; i++ {
					ref := fmt.Sprintf("ref-%d", i)
					gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(ref), commit)
					input.WriteString(ref)
					input.WriteString("\n")
				}

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields logrus.Fields) {
				require.Equal(t, 2, statFields["pack_objects.written_object_count"])
			},
		},
		{
			desc: "pack tags",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("commit"), gittest.WithBranch("main"))
				for i := 0; i <= 10; i++ {
					tag := fmt.Sprintf("tag-%d", i)
					gittest.WriteTag(t, cfg, repoPath, tag, commit.Revision())
					input.WriteString(tag)
					input.WriteString("\n")
				}

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields logrus.Fields) {
				require.Equal(t, 2, statFields["pack_objects.written_object_count"])
			},
		},
		{
			desc: "pack tree",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				tree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "file1",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "file2",
					},
					{
						Mode:    "100644",
						Path:    "file3",
						Content: "file3",
					},
					{
						OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{
								Mode:    "100644",
								Path:    "subfile1",
								Content: "subfile1",
							},
							{
								Mode:    "100644",
								Path:    "subfile2",
								Content: "subfile2",
							},
						}),
						Mode: "040000",
						Path: "subdir",
					},
				})
				input.WriteString(tree.String())
				input.WriteString("\n")

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields logrus.Fields) {
				require.Equal(t, 7, statFields["pack_objects.written_object_count"])
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := command.InitContextStats(testhelper.Context(t))
			repoProto, cfg, input := tc.setupRepo(ctx)
			gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, git.WithTrace2Hooks([]trace2.Hook{
				trace2hooks.NewPackObjectsMetrics(),
			}))
			require.NoError(t, err)
			defer cleanup()

			cmd, err := gitCmdFactory.New(ctx, repoProto, git.Command{
				Name: "pack-objects",
				Flags: []git.Option{
					git.Flag{Name: "--compression=0"},
					git.Flag{Name: "--stdout"},
					git.Flag{Name: "--unpack-unreachable"},
					git.Flag{Name: "-q"},
				},
			}, git.WithStdin(&input))
			require.NoError(t, err)

			err = cmd.Wait()
			require.NoError(t, err)

			stats := command.StatsFromContext(ctx)
			require.NotNil(t, stats)

			statFields := stats.Fields()
			require.Equal(t, "true", statFields["trace2.activated"])
			require.Equal(t, "pack_objects_metrics", statFields["trace2.hooks"])
			require.Contains(t, statFields, "pack_objects.enumerate_objects_ms")
			require.Contains(t, statFields, "pack_objects.prepare_pack_ms")
			require.Contains(t, statFields, "pack_objects.write_pack_file_ms")

			tc.assert(t, statFields)
		})
	}
}
