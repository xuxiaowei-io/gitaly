package main

import (
	"os"

	cli "gitlab.com/gitlab-org/gitaly/v16/internal/cli/gitalybackup"
)

func main() {
	if err := cli.NewApp().Run(os.Args); err != nil {
		os.Exit(1)
	}
}
