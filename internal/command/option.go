package command

import "io"

type config struct {
	stdin       io.Reader
	stdout      io.Writer
	stderr      io.Writer
	environment []string
}

// Option is an option that can be passed to `New()` for controlling how the command is being
// created.
type Option func(cfg *config)

// WithStdin sets up the command to read from the given reader. If stdin is specified as SetupStdin,
// you will be able to write to the stdin of the subprocess by calling Write() on the returned
// Command.
func WithStdin(stdin io.Reader) Option {
	return func(cfg *config) {
		cfg.stdin = stdin
	}
}

// WithStdout sets up the command to write standard output to the given writer.
func WithStdout(stdout io.Writer) Option {
	return func(cfg *config) {
		cfg.stdout = stdout
	}
}

// WithStderr sets up the command to write standard error to the given writer.
func WithStderr(stderr io.Writer) Option {
	return func(cfg *config) {
		cfg.stderr = stderr
	}
}

// WithEnvironment sets up environment variables that shall be set for the command.
func WithEnvironment(environment []string) Option {
	return func(cfg *config) {
		cfg.environment = environment
	}
}
