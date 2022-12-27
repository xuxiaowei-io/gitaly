package catfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type repoExecutor struct {
	repository.GitRepo
	gitCmdFactory git.CommandFactory
}

func newRepoExecutor(t *testing.T, cfg config.Cfg, repo repository.GitRepo) git.RepositoryExecutor {
	return &repoExecutor{
		GitRepo:       repo,
		gitCmdFactory: git.NewCommandFactory(t, cfg),
	}
}

func (e *repoExecutor) Exec(ctx context.Context, cmd git.Command, opts ...git.CmdOpt) (*command.Command, error) {
	return e.gitCmdFactory.New(ctx, e.GitRepo, cmd, opts...)
}

func (e *repoExecutor) ExecAndWait(ctx context.Context, cmd git.Command, opts ...git.CmdOpt) error {
	command, err := e.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}
	return command.Wait()
}

func (e *repoExecutor) GitVersion(ctx context.Context) (git.Version, error) {
	return git.Version{}, nil
}

func (e *repoExecutor) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	return git.DefaultObjectHash, nil
}

func setupObjectReader(t *testing.T, ctx context.Context) (config.Cfg, ObjectContentReader, *gitalypb.Repository, string) {
	t.Helper()

	cfg := testcfg.Build(t)
	repo, repoPath := git.CreateRepository(t, ctx, cfg, git.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repoExecutor := newRepoExecutor(t, cfg, repo)

	cache := newCache(1*time.Hour, 1000, helper.NewTimerTicker(defaultEvictionInterval))
	t.Cleanup(cache.Stop)

	objectReader, cancel, err := cache.ObjectReader(ctx, repoExecutor)
	require.NoError(t, err)
	t.Cleanup(cancel)

	return cfg, objectReader, repo, repoPath
}

type staticObject struct {
	reader     io.Reader
	objectType string
	objectSize int64
	objectID   git.ObjectID
}

func newStaticObject(contents string, objectType string, oid git.ObjectID) *staticObject {
	return &staticObject{
		reader:     strings.NewReader(contents),
		objectType: objectType,
		objectSize: int64(len(contents)),
		objectID:   oid,
	}
}

func (o *staticObject) ObjectID() git.ObjectID {
	return o.objectID
}

func (o *staticObject) ObjectSize() int64 {
	return o.objectSize
}

func (o *staticObject) ObjectType() string {
	return o.objectType
}

func (o *staticObject) Read(p []byte) (int, error) {
	return o.reader.Read(p)
}

func (o *staticObject) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, o.reader)
}

var (
	// defaultCommitterName is the default name of the committer and author used to create
	// commits.
	defaultCommitterName = "Scrooge McDuck"
	// defaultCommitterMail is the default mail of the committer and author used to create
	// commits.
	defaultCommitterMail = "scrooge@mcduck.com"
	// defaultCommitTime is the default time used as written by WriteCommit().
	defaultCommitTime = time.Date(2019, 11, 3, 11, 27, 59, 0, time.FixedZone("", 60*60))
	// defaultCommitterSignature is the default signature in the format like it would be present
	// in commits: "$name <$email> $unixtimestamp $timezone".
	defaultCommitterSignature = fmt.Sprintf(
		"%s <%s> %d %s", defaultCommitterName, defaultCommitterMail, defaultCommitTime.Unix(), defaultCommitTime.Format("-0700"),
	)
	// defaultCommitAuthor is the Protobuf message representation of the default committer and
	// author used to create commits.
	defaultCommitAuthor = &gitalypb.CommitAuthor{
		Name:     []byte(defaultCommitterName),
		Email:    []byte(defaultCommitterMail),
		Date:     timestamppb.New(defaultCommitTime),
		Timezone: []byte("+0100"),
	}
)

type writeCommitConfig struct {
	reference          string
	parents            []git.ObjectID
	authorDate         time.Time
	authorName         string
	committerName      string
	committerDate      time.Time
	message            string
	treeEntries        []git.TreeEntry
	treeID             git.ObjectID
	alternateObjectDir string
}

// writeCommitOption is an option which can be passed to WriteCommit.
type writeCommitOption func(*writeCommitConfig)

func withReference(reference string) writeCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.reference = reference
	}
}

func withBranch(branch string) writeCommitOption {
	return withReference("refs/heads/" + branch)
}

func withTreeEntries(entries ...git.TreeEntry) writeCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.treeEntries = entries
	}
}

func withTree(treeID git.ObjectID) writeCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.treeID = treeID
	}
}

func withMessage(message string) writeCommitOption {
	return func(cfg *writeCommitConfig) {
		cfg.message = message
	}
}

// writeTestCommit writes a new commit into the target repository.
func writeTestCommit(tb testing.TB, cfg config.Cfg, repoPath string, opts ...writeCommitOption) git.ObjectID {
	tb.Helper()

	var writeCommitConfig writeCommitConfig
	for _, opt := range opts {
		opt(&writeCommitConfig)
	}

	message := "message"
	if writeCommitConfig.message != "" {
		message = writeCommitConfig.message
	}
	stdin := bytes.NewBufferString(message)

	if len(writeCommitConfig.treeEntries) > 0 && writeCommitConfig.treeID != "" {
		require.FailNow(tb, "cannot set tree entries and tree ID at the same time")
	}

	var tree string
	if writeCommitConfig.treeEntries != nil {
		tree = git.WriteTree(tb, cfg, repoPath, writeCommitConfig.treeEntries).String()
	} else if writeCommitConfig.treeID != "" {
		tree = writeCommitConfig.treeID.String()
	} else if len(writeCommitConfig.parents) == 0 {
		tree = git.WriteTree(tb, cfg, repoPath, []git.TreeEntry{}).String()
	}

	if writeCommitConfig.authorName == "" {
		writeCommitConfig.authorName = defaultCommitterName
	}

	if writeCommitConfig.authorDate.IsZero() {
		writeCommitConfig.authorDate = time.Date(2019, 11, 3, 11, 27, 59, 0, time.FixedZone("UTC+1", 1*60*60))
	}

	if writeCommitConfig.committerName == "" {
		writeCommitConfig.committerName = defaultCommitterName
	}

	if writeCommitConfig.committerDate.IsZero() {
		writeCommitConfig.committerDate = time.Date(2019, 11, 3, 11, 27, 59, 0, time.FixedZone("UTC+1", 1*60*60))
	}

	// Use 'commit-tree' instead of 'commit' because we are in a bare
	// repository. What we do here is the same as "commit -m message
	// --allow-empty".
	commitArgs := []string{
		"-c", fmt.Sprintf("user.name=%s", writeCommitConfig.committerName),
		"-c", fmt.Sprintf("user.email=%s", defaultCommitterMail),
		"-C", repoPath,
		"commit-tree", "-F", "-", tree,
	}

	var env []string
	if writeCommitConfig.alternateObjectDir != "" {
		require.True(tb, filepath.IsAbs(writeCommitConfig.alternateObjectDir),
			"alternate object directory must be an absolute path")
		require.NoError(tb, os.MkdirAll(writeCommitConfig.alternateObjectDir, 0o755))

		env = append(env,
			fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", writeCommitConfig.alternateObjectDir),
			fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", filepath.Join(repoPath, "objects")),
		)
	}

	env = append(env,
		fmt.Sprintf("GIT_AUTHOR_DATE=%d %s", writeCommitConfig.authorDate.Unix(), writeCommitConfig.authorDate.Format("-0700")),
		fmt.Sprintf("GIT_AUTHOR_NAME=%s", writeCommitConfig.authorName),
		fmt.Sprintf("GIT_AUTHOR_EMAIL=%s", defaultCommitterMail),
		fmt.Sprintf("GIT_COMMITTER_DATE=%d %s", writeCommitConfig.committerDate.Unix(), writeCommitConfig.committerDate.Format("-0700")),
		fmt.Sprintf("GIT_COMMITTER_NAME=%s", writeCommitConfig.committerName),
		fmt.Sprintf("GIT_COMMITTER_EMAIL=%s", defaultCommitterMail),
	)

	for _, parent := range writeCommitConfig.parents {
		commitArgs = append(commitArgs, "-p", parent.String())
	}

	stdout := git.ExecOpts(tb, cfg, git.ExecConfig{
		Stdin: stdin,
		Env:   env,
	}, commitArgs...)
	oid, err := git.DefaultObjectHash.FromHex(text.ChompBytes(stdout))
	require.NoError(tb, err)

	if writeCommitConfig.reference != "" {
		git.ExecOpts(tb, cfg, git.ExecConfig{
			Env: env,
		}, "-C", repoPath, "update-ref", writeCommitConfig.reference, oid.String())
	}

	return oid
}
