package catfile

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type RepoExecutor struct {
	repository.GitRepo
	GitCmdFactory git.CommandFactory
}

func NewRepoExecutor(t *testing.T, cfg config.Cfg, repo repository.GitRepo) git.RepositoryExecutor {
	return &RepoExecutor{
		GitRepo:       repo,
		GitCmdFactory: gittest.NewCommandFactory(t, cfg),
	}
}

func (e *RepoExecutor) Exec(ctx context.Context, cmd git.Command, opts ...git.CmdOpt) (*command.Command, error) {
	return e.GitCmdFactory.New(ctx, e.GitRepo, cmd, opts...)
}

func (e *RepoExecutor) ExecAndWait(ctx context.Context, cmd git.Command, opts ...git.CmdOpt) error {
	command, err := e.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}
	return command.Wait()
}

func (e *RepoExecutor) GitVersion(ctx context.Context) (git.Version, error) {
	return git.Version{}, nil
}

func (e *RepoExecutor) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	return gittest.DefaultObjectHash, nil
}

func setupObjectReader(t *testing.T, ctx context.Context) (config.Cfg, ObjectContentReader, *gitalypb.Repository, string) {
	t.Helper()

	cfg := testcfg.Build(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	RepoExecutor := NewRepoExecutor(t, cfg, repo)

	cache := newCache(1*time.Hour, 1000, helper.NewTimerTicker(defaultEvictionInterval))
	t.Cleanup(cache.Stop)

	objectReader, cancel, err := cache.ObjectReader(ctx, RepoExecutor)
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

func (q *requestQueue) IsDirty() bool {
	return q.isDirty()
}

func (q *requestQueue) Close() {
	q.close()
}

//var NewInterceptedInfoQueue = newInterceptedInfoQueue

var NewObjectContentReader = newObjectContentReader

func (o *objectContentReader) Close() {
	o.close()
}
