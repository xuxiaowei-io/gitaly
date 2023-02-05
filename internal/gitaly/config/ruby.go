package config

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
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

// ConfigureRuby validates the gitaly-ruby configuration and sets default values.
func (cfg *Cfg) ConfigureRuby() error {
	var errs ValidationErrors

	if timeout := cfg.Ruby.GracefulRestartTimeout.Duration(); timeout == 0 {
		cfg.Ruby.GracefulRestartTimeout = duration.Duration(10 * time.Minute)
	} else if timeout < 0 {
		errs = append(errs, ValidationError{
			Key:     []string{"gitaly-ruby", "graceful_restart_timeout"},
			Message: "cannot be negative",
		})
	}

	if maxRSS := cfg.Ruby.MaxRSS; maxRSS == 0 {
		cfg.Ruby.MaxRSS = 200 * 1024 * 1024
	}

	if delay := cfg.Ruby.RestartDelay.Duration(); delay == 0 {
		cfg.Ruby.RestartDelay = duration.Duration(5 * time.Minute)
	} else if delay < 0 {
		errs = append(errs, ValidationError{
			Key:     []string{"gitaly-ruby", "restart_delay"},
			Message: "cannot be negative",
		})
	}

	if minWorkers := uint(2); cfg.Ruby.NumWorkers < minWorkers {
		cfg.Ruby.NumWorkers = minWorkers
	}

	if len(cfg.Ruby.Dir) == 0 {
		errs = append(errs, ValidationError{
			Key:     []string{"gitaly-ruby", "dir"},
			Message: "is not set",
		})
	} else {
		rubyDir, err := filepath.Abs(cfg.Ruby.Dir)
		if err != nil {
			errs = append(errs, ValidationError{
				Key:     []string{"gitaly-ruby", "dir"},
				Message: fmt.Sprintf("'%s' %s", cfg.Ruby.Dir, err),
			})
		} else if err := validateIsDirectory(rubyDir); err != nil {
			errs = append(errs, ValidationError{
				Key:     []string{"gitaly-ruby", "dir"},
				Message: err.Error(),
			})
		} else {
			cfg.Ruby.Dir = rubyDir
			log.WithField("dir", cfg.Ruby.Dir).Debug("gitaly-ruby.dir set")
		}
	}

	if path := strings.TrimSpace(cfg.Ruby.RuggedGitConfigSearchPath); path != "" {
		ruggedPath, err := filepath.Abs(path)
		if err != nil {
			errs = append(errs, ValidationError{
				Key:     []string{"gitaly-ruby", "rugged_git_config_search_path"},
				Message: fmt.Sprintf("'%s' %s", path, err),
			})
		} else {
			cfg.Ruby.RuggedGitConfigSearchPath = ruggedPath
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}
