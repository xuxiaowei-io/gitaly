package localrepo

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// Repo represents a local Git repository.
type Repo struct {
	repository.GitRepo
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache

	detectObjectHashOnce sync.Once
	objectHash           git.ObjectHash
	objectHashErr        error
}

// New creates a new Repo from its protobuf representation.
func New(locator storage.Locator, gitCmdFactory git.CommandFactory, catfileCache catfile.Cache, repo repository.GitRepo) *Repo {
	return &Repo{
		GitRepo:       repo,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
	}
}

// Quarantine return the repository quarantined. The quarantine directory becomes the repository's
// main object directory and the original object directory is configured as an alternate.
func (repo *Repo) Quarantine(quarantineDirectory string) (*Repo, error) {
	pbRepo, ok := repo.GitRepo.(*gitalypb.Repository)
	if !ok {
		return nil, fmt.Errorf("unexpected repository type %t", repo.GitRepo)
	}

	repoPath, err := repo.Path()
	if err != nil {
		return nil, fmt.Errorf("repo path: %w", err)
	}

	quarantinedRepo, err := quarantine.Apply(repoPath, pbRepo, quarantineDirectory)
	if err != nil {
		return nil, fmt.Errorf("apply quarantine: %w", err)
	}

	return New(
		repo.locator,
		repo.gitCmdFactory,
		repo.catfileCache,
		quarantinedRepo,
	), nil
}

// NewTestRepo constructs a Repo. It is intended as a helper function for tests which assembles
// dependencies ad-hoc from the given config.
func NewTestRepo(tb testing.TB, cfg config.Cfg, repo repository.GitRepo, factoryOpts ...git.ExecCommandFactoryOption) *Repo {
	tb.Helper()

	if cfg.SocketPath != testcfg.UnconfiguredSocketPath {
		repo = gittest.RewrittenRepository(tb, testhelper.Context(tb), cfg, &gitalypb.Repository{
			StorageName:                   repo.GetStorageName(),
			RelativePath:                  repo.GetRelativePath(),
			GitObjectDirectory:            repo.GetGitObjectDirectory(),
			GitAlternateObjectDirectories: repo.GetGitAlternateObjectDirectories(),
		})
	}

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, factoryOpts...)
	tb.Cleanup(cleanup)
	require.NoError(tb, err)

	catfileCache := catfile.NewCache(cfg)
	tb.Cleanup(catfileCache.Stop)

	locator := config.NewLocator(cfg)

	return New(locator, gitCmdFactory, catfileCache, repo)
}

// Exec creates a git command with the given args and Repo, executed in the
// Repo. It validates the arguments in the command before executing.
func (repo *Repo) Exec(ctx context.Context, cmd git.Command, opts ...git.CmdOpt) (*command.Command, error) {
	return repo.gitCmdFactory.New(ctx, repo, cmd, opts...)
}

// ExecAndWait is similar to Exec, but waits for the command to exit before
// returning.
func (repo *Repo) ExecAndWait(ctx context.Context, cmd git.Command, opts ...git.CmdOpt) error {
	command, err := repo.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}

	return command.Wait()
}

// GitVersion returns the Git version in use.
func (repo *Repo) GitVersion(ctx context.Context) (git.Version, error) {
	return repo.gitCmdFactory.GitVersion(ctx)
}

func errorWithStderr(err error, stderr []byte) error {
	if len(stderr) == 0 {
		return err
	}
	return fmt.Errorf("%w, stderr: %q", err, stderr)
}

// StorageTempDir returns the temporary dir for the storage where the repo is on.
// When this directory does not exist yet, it's being created.
func (repo *Repo) StorageTempDir() (string, error) {
	tempPath, err := repo.locator.TempDir(repo.GetStorageName())
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(tempPath, perm.SharedDir); err != nil {
		return "", err
	}

	return tempPath, nil
}

// ObjectHash detects the object hash used by this particular repository.
func (repo *Repo) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	repo.detectObjectHashOnce.Do(func() {
		repo.objectHash, repo.objectHashErr = git.DetectObjectHash(ctx, repo)
	})
	return repo.objectHash, repo.objectHashErr
}
