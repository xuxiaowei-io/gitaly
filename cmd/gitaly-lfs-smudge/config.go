package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

type configProvider interface {
	Get(key string) string
}

type envConfig struct{}

func (e *envConfig) Get(key string) string {
	return os.Getenv(key)
}

func loadConfig(cfgProvider configProvider) (config.Gitlab, config.TLS, string, error) {
	var cfg config.Gitlab
	var tlsCfg config.TLS

	glRepository := cfgProvider.Get("GL_REPOSITORY")
	if glRepository == "" {
		return cfg, tlsCfg, "", fmt.Errorf("error loading project: GL_REPOSITORY is not defined")
	}

	u := cfgProvider.Get("GL_INTERNAL_CONFIG")
	if u == "" {
		return cfg, tlsCfg, glRepository, fmt.Errorf("unable to retrieve GL_INTERNAL_CONFIG")
	}

	if err := json.Unmarshal([]byte(u), &cfg); err != nil {
		return cfg, tlsCfg, glRepository, fmt.Errorf("unable to unmarshal GL_INTERNAL_CONFIG: %v", err)
	}

	u = cfgProvider.Get("GITALY_TLS")
	if u == "" {
		return cfg, tlsCfg, glRepository, errors.New("unable to retrieve GITALY_TLS")
	}

	if err := json.Unmarshal([]byte(u), &tlsCfg); err != nil {
		return cfg, tlsCfg, glRepository, fmt.Errorf("unable to unmarshal GITALY_TLS: %w", err)
	}

	return cfg, tlsCfg, glRepository, nil
}
