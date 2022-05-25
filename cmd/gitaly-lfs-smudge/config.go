package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/env"
)

// Config is the configuration used to run gitaly-lfs-smudge. It must be injected via environment
// variables.
type Config struct {
	// GlRepository is the GitLab repository identifier that is required so that we can query
	// the corresponding Rails' repository for the respective LFS contents.
	GlRepository string
	// Gitlab contains configuration so that we can connect to Rails in order to retrieve LFS
	// contents.
	Gitlab config.Gitlab
	// TLS contains configuration for setting up a TLS-encrypted connection to Rails.
	TLS config.TLS
}

func configFromEnvironment(environment []string) (Config, error) {
	var cfg Config

	cfg.GlRepository = env.ExtractValue(environment, "GL_REPOSITORY")
	if cfg.GlRepository == "" {
		return Config{}, fmt.Errorf("error loading project: GL_REPOSITORY is not defined")
	}

	u := env.ExtractValue(environment, "GL_INTERNAL_CONFIG")
	if u == "" {
		return Config{}, fmt.Errorf("unable to retrieve GL_INTERNAL_CONFIG")
	}

	if err := json.Unmarshal([]byte(u), &cfg.Gitlab); err != nil {
		return Config{}, fmt.Errorf("unable to unmarshal GL_INTERNAL_CONFIG: %w", err)
	}

	u = env.ExtractValue(environment, "GITALY_TLS")
	if u == "" {
		return Config{}, errors.New("unable to retrieve GITALY_TLS")
	}

	if err := json.Unmarshal([]byte(u), &cfg.TLS); err != nil {
		return Config{}, fmt.Errorf("unable to unmarshal GITALY_TLS: %w", err)
	}

	return cfg, nil
}
