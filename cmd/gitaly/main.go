package main

import (
	"log"
	"os"

	cli "gitlab.com/gitlab-org/gitaly/v16/internal/cli/gitaly"
)

// This binary is used to support signing through providing the path to the
// actual signing key. Git itself does not support this since it simply calls
// out to gpg(1), which only supports fetching keys from the gpg database.
// This binary is used as a stopgap meaure since we can set Git's gpg.program
// config to point to this binary, which interprets the key_id passed in as the
// path to the signing key.
// In the future, we will modify Git so that commit-tree can take in a raw
// commit message that we can add a signature to, at which point we can sign
// commits manually and get rid of this binary.
func main() {
	if err := cli.NewApp().Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
