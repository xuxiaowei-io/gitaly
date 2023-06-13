package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gpg"
)

func gpgApp() *cli.App {
	return &cli.App{
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "status-fd"},
			&cli.BoolFlag{Name: "verify"},
			&cli.BoolFlag{Name: "bsau"},
		},
		Action: func(cCtx *cli.Context) error {
			// Git passes the --status-fd=2 flag into the gpg call.
			if cCtx.Int("status-fd") != 2 {
				return errors.New("expected --status-fd=2")
			}

			signedKeyData, err := os.ReadFile(cCtx.Args().First())
			if err != nil {
				return fmt.Errorf("reading signed key file %s : %w", cCtx.Args().First(), err)
			}

			contents, err := io.ReadAll(cCtx.App.Reader)
			if err != nil {
				return fmt.Errorf("reading contents from stdin: %w", err)
			}

			signature, err := gpg.CreateSignature(signedKeyData, contents)
			if err != nil {
				return fmt.Errorf("creating signature: %w", err)
			}

			// Git looks for this output string as part of GPG output.
			if _, err := cCtx.App.ErrWriter.Write([]byte("[GNUPG:] SIG_CREATED ")); err != nil {
				return fmt.Errorf("printing to stdout: %w", err)
			}

			if _, err := cCtx.App.Writer.Write(signature); err != nil {
				return fmt.Errorf("printing to stdout: %w", err)
			}

			if _, err := cCtx.App.Writer.Write([]byte("\n")); err != nil {
				return fmt.Errorf("printing to stdout: %w", err)
			}

			return nil
		},
	}
}

func main() {
	if err := gpgApp().Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
