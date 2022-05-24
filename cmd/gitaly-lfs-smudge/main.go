package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	gitalylog "gitlab.com/gitlab-org/gitaly/v15/internal/log"
	"gitlab.com/gitlab-org/labkit/log"
)

type envConfig struct{}

func (e *envConfig) Get(key string) string {
	return os.Getenv(key)
}

func requireStdin(msg string) {
	var out string

	stat, err := os.Stdin.Stat()
	if err != nil {
		out = fmt.Sprintf("Cannot read from STDIN. %s (%s)", msg, err)
	} else if (stat.Mode() & os.ModeCharDevice) != 0 {
		out = fmt.Sprintf("Cannot read from STDIN. %s", msg)
	}

	if len(out) > 0 {
		fmt.Println(out)
		os.Exit(1)
	}
}

func main() {
	requireStdin("This command should be run by the Git 'smudge' filter")

	closer, err := initLogging(&envConfig{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error initializing log file for gitaly-lfs-smudge: %v", err)
	}
	defer closer.Close()

	if err := smudge(os.Stdout, os.Stdin, &envConfig{}); err != nil {
		log.WithError(err).Error(err)
		os.Exit(1)
	}
}

func initLogging(p configProvider) (io.Closer, error) {
	path := p.Get(gitalylog.GitalyLogDirEnvKey)
	if path == "" {
		return log.Initialize(log.WithWriter(io.Discard))
	}

	filepath := filepath.Join(path, "gitaly_lfs_smudge.log")

	return log.Initialize(
		log.WithFormatter("json"),
		log.WithLogLevel("info"),
		log.WithOutputName(filepath),
	)
}
