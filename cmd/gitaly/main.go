package main

import (
	"log"
	"os"

	cli "gitlab.com/gitlab-org/gitaly/v15/internal/cli/gitaly"
)

func main() {
	if err := cli.NewApp().Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
