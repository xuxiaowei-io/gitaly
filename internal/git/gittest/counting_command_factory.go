package gittest

import (
	"context"
	"sync"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

var _ git.CommandFactory = &CountingCommandFactory{}

// CountingCommandFactory embeds a regular git command factory, but it keeps
// count of each call of New*() with their git command.
type CountingCommandFactory struct {
	git.CommandFactory

	counts map[string]uint64
	m      sync.Mutex
}

// NewCountingCommandFactory creates a CountingCommandFactory
func NewCountingCommandFactory(tb testing.TB, cfg config.Cfg, opts ...git.ExecCommandFactoryOption) *CountingCommandFactory {
	return &CountingCommandFactory{
		CommandFactory: NewCommandFactory(tb, cfg, opts...),
		counts:         make(map[string]uint64),
	}
}

// CommandCount returns the current count
func (f *CountingCommandFactory) CommandCount(cmd string) uint64 {
	f.m.Lock()
	defer f.m.Unlock()

	c := f.counts[cmd]

	return c
}

// ResetCount resets all counts to zero
func (f *CountingCommandFactory) ResetCount() {
	f.m.Lock()
	defer f.m.Unlock()

	f.counts = make(map[string]uint64)
}

// New creates a new git command and increments the command counter
func (f *CountingCommandFactory) New(ctx context.Context, repo repository.GitRepo, sc git.Command, opts ...git.CmdOpt) (*command.Command, error) {
	f.m.Lock()
	defer f.m.Unlock()
	f.counts[sc.Name]++

	return f.CommandFactory.New(ctx, repo, sc, opts...)
}

// NewWithoutRepo creates a new git command and increments the command counter
func (f *CountingCommandFactory) NewWithoutRepo(ctx context.Context, sc git.Command, opts ...git.CmdOpt) (*command.Command, error) {
	f.m.Lock()
	defer f.m.Unlock()
	f.counts[sc.Name]++

	return f.CommandFactory.NewWithoutRepo(ctx, sc, opts...)
}
