package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

type subcmd interface {
	Flags(*flag.FlagSet)
	Run(ctx context.Context, logger logrus.FieldLogger, stdin io.Reader, stdout io.Writer) error
}

var subcommands = map[string]subcmd{
	"create":  &createSubcommand{},
	"restore": &restoreSubcommand{},
}

func main() {
	logger, err := log.Configure(os.Stdout, "json", "")
	if err != nil {
		fmt.Printf("configuring logger failed: %v", err)
		os.Exit(1)
	}

	flags := flag.NewFlagSet("gitaly-backup", flag.ExitOnError)
	_ = flags.Parse(os.Args)

	if flags.NArg() < 2 {
		logger.Fatal("missing subcommand")
	}

	subcmdName := flags.Arg(1)
	subcmd, ok := subcommands[subcmdName]
	if !ok {
		logger.Fatalf("unknown subcommand: %q", flags.Arg(1))
	}

	subcmdFlags := flag.NewFlagSet(subcmdName, flag.ExitOnError)
	subcmd.Flags(subcmdFlags)
	_ = subcmdFlags.Parse(flags.Args()[2:])

	ctx, err := storage.InjectGitalyServersEnv(context.Background())
	if err != nil {
		logger.Fatalf("%s", err)
	}

	if err := subcmd.Run(ctx, logger, os.Stdin, os.Stdout); err != nil {
		logger.Fatalf("%s", err)
	}
}
