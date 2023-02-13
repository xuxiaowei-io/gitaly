package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v15/internal/log"
	"google.golang.org/grpc/metadata"
)

type subcmd interface {
	Flags(*flag.FlagSet)
	Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error
}

var subcommands = map[string]subcmd{
	"create":  &createSubcommand{},
	"restore": &restoreSubcommand{},
}

func main() {
	log.Configure(log.Loggers, "json", "")

	logger := log.Default()

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

	ctx, err := injectGitalyServersEnv(context.Background())
	if err != nil {
		logger.Fatalf("%s", err)
	}

	if err := subcmd.Run(ctx, os.Stdin, os.Stdout); err != nil {
		logger.Fatalf("%s", err)
	}
}

func injectGitalyServersEnv(ctx context.Context) (context.Context, error) {
	rawServers := env.GetString("GITALY_SERVERS", "")
	if rawServers == "" {
		return ctx, nil
	}

	md := metadata.Pairs("gitaly-servers", rawServers)
	ctx = metadata.NewIncomingContext(ctx, md)

	// Make sure we fail early if the value in the env var cannot be interpreted.
	if _, err := storage.ExtractGitalyServers(ctx); err != nil {
		return nil, fmt.Errorf("injecting GITALY_SERVERS: %w", err)
	}

	return ctx, nil
}
