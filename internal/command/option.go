package command

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
)

type config struct {
	stdin       io.Reader
	stdout      io.Writer
	stderr      io.Writer
	dir         string
	environment []string

	finalizers []func(context.Context, *Command)

	commandName    string
	subcommandName string
	gitVersion     string

	cgroupsManager        cgroups.Manager
	cgroupsAddCommandOpts []cgroups.AddCommandOption
	spawnTokenManager     *SpawnTokenManager
}

// Option is an option that can be passed to `New()` for controlling how the command is being
// created.
type Option func(cfg *config)

// WithStdin sets up the command to read from the given reader.
func WithStdin(stdin io.Reader) Option {
	return func(cfg *config) {
		cfg.stdin = stdin
	}
}

// WithSetupStdin instructs New() to configure the stdin pipe of the command it is creating. This
// allows you call Write() on the command as if it is an ordinary io.Writer, sending data directly
// to the stdin of the process.
func WithSetupStdin() Option {
	return func(cfg *config) {
		cfg.stdin = stdinSentinel{}
	}
}

// WithStdout sets up the command to write standard output to the given writer.
func WithStdout(stdout io.Writer) Option {
	return func(cfg *config) {
		cfg.stdout = stdout
	}
}

// WithSetupStdout instructs New() to configure the standard output pipe of the command it is creating. This allowsyou
// to call Read() on the command as if it is an ordinary io.Reader, reading output directly from the stdout of the
// process.
func WithSetupStdout() Option {
	return func(cfg *config) {
		cfg.stdout = stdoutSentinel{}
	}
}

// WithStderr sets up the command to write standard error to the given writer.
func WithStderr(stderr io.Writer) Option {
	return func(cfg *config) {
		cfg.stderr = stderr
	}
}

// WithDir will set up the command to be ran in the specific directory.
func WithDir(dir string) Option {
	return func(cfg *config) {
		cfg.dir = dir
	}
}

// WithEnvironment sets up environment variables that shall be set for the command.
func WithEnvironment(environment []string) Option {
	return func(cfg *config) {
		cfg.environment = environment
	}
}

// WithCommandName overrides the "cmd" and "subcmd" label used in metrics.
func WithCommandName(commandName, subcommandName string) Option {
	return func(cfg *config) {
		cfg.commandName = commandName
		cfg.subcommandName = subcommandName
	}
}

// WithCommandGitVersion overrides the "git_version" label used in metrics.
func WithCommandGitVersion(gitCmdVersion string) Option {
	return func(cfg *config) {
		cfg.gitVersion = gitCmdVersion
	}
}

// WithCgroup adds the spawned command to a Cgroup. The bucket used will be derived from the
// command's arguments and/or from the repository.
func WithCgroup(cgroupsManager cgroups.Manager, opts ...cgroups.AddCommandOption) Option {
	return func(cfg *config) {
		cfg.cgroupsManager = cgroupsManager
		cfg.cgroupsAddCommandOpts = opts
	}
}

// WithSpawnTokenManager assigns a spawn token manager for the command. If this option is not set, the command uses
// the process-global spawn token manager.
func WithSpawnTokenManager(spawnTokenManager *SpawnTokenManager) Option {
	return func(cfg *config) {
		cfg.spawnTokenManager = spawnTokenManager
	}
}

// WithFinalizer sets up the finalizer to be run when the command is being wrapped up. It will be
// called after `Wait()` has returned.
func WithFinalizer(finalizer func(context.Context, *Command)) Option {
	return func(cfg *config) {
		cfg.finalizers = append(cfg.finalizers, finalizer)
	}
}
