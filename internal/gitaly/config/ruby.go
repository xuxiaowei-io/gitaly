package config

import (
	"fmt"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/errors/cfgerror"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/duration"
)

// Ruby contains setting for Ruby worker processes
type Ruby struct {
	Dir                       string            `toml:"dir"`
	MaxRSS                    uint              `toml:"max_rss"`
	GracefulRestartTimeout    duration.Duration `toml:"graceful_restart_timeout"`
	RestartDelay              duration.Duration `toml:"restart_delay"`
	NumWorkers                uint              `toml:"num_workers"`
	RuggedGitConfigSearchPath string            `toml:"rugged_git_config_search_path"`
}

// Validate runs validation on all fields and compose all found errors.
func (r Ruby) Validate() error {
	return cfgerror.New().
		Append(cfgerror.IsPositive(r.GracefulRestartTimeout.Duration()), "graceful_restart_timeout").
		Append(cfgerror.IsPositive(r.RestartDelay.Duration()), "restart_delay").
		Append(cfgerror.DirExists(r.Dir), "dir").
		AsError()
}

// ConfigureRuby validates the gitaly-ruby configuration and sets default values.
func (cfg *Cfg) ConfigureRuby() error {
	if cfg.Ruby.GracefulRestartTimeout.Duration() == 0 {
		cfg.Ruby.GracefulRestartTimeout = duration.Duration(10 * time.Minute)
	}

	if cfg.Ruby.MaxRSS == 0 {
		cfg.Ruby.MaxRSS = 200 * 1024 * 1024
	}

	if cfg.Ruby.RestartDelay.Duration() == 0 {
		cfg.Ruby.RestartDelay = duration.Duration(5 * time.Minute)
	}

	if len(cfg.Ruby.Dir) == 0 {
		return fmt.Errorf("gitaly-ruby.dir: is not set")
	}

	minWorkers := uint(2)
	if cfg.Ruby.NumWorkers < minWorkers {
		cfg.Ruby.NumWorkers = minWorkers
	}

	var err error
	cfg.Ruby.Dir, err = filepath.Abs(cfg.Ruby.Dir)
	if err != nil {
		return err
	}

	if len(cfg.Ruby.RuggedGitConfigSearchPath) != 0 {
		cfg.Ruby.RuggedGitConfigSearchPath, err = filepath.Abs(cfg.Ruby.RuggedGitConfigSearchPath)
		if err != nil {
			return err
		}
	}

	return validateIsDirectory(cfg.Ruby.Dir, "gitaly-ruby.dir")
}
