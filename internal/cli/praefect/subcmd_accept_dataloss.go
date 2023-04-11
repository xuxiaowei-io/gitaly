package praefect

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/log"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// praefect -config PATH_TO_CONFIG accept-dataloss -virtual-storage <virtual-storage> -relative-path <relative-path> -authoritative-storage <authoritative-storage>
func newAcceptDatalossCommand() *cli.Command {
	return &cli.Command{
		Name:  "accept-dataloss",
		Usage: "allows for accepting data loss in a repository",
		Description: `The subcommand "accept-dataloss" allows for accepting data loss in a repository to enable it for
writing again. The current version of the repository on the authoritative storage is set to be
the latest version and replications to other nodes are scheduled in order to bring them consistent
with the new authoritative version.
`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     paramVirtualStorage,
				Usage:    "name of the repository's virtual storage",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramRelativePath,
				Usage:    "repository to accept data loss for",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramAuthoritativeStorage,
				Usage:    "storage with the repository to consider as authoritative",
				Required: true,
			},
		},
		Action: acceptDatalossAction,
		Before: func(context *cli.Context) error {
			if context.Args().Present() {
				return unexpectedPositionalArgsError{Command: context.Command.Name}
			}
			return nil
		},
	}
}

func acceptDatalossAction(ctx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, ctx.String(configFlagName))
	if err != nil {
		return err
	}

	nodeAddr, err := getNodeAddress(conf)
	if err != nil {
		return err
	}

	conn, err := subCmdDial(ctx.Context, nodeAddr, conf.Auth.Token, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("error dialing: %w", err)
	}
	defer conn.Close()

	client := gitalypb.NewPraefectInfoServiceClient(conn)
	if _, err := client.SetAuthoritativeStorage(ctx.Context, &gitalypb.SetAuthoritativeStorageRequest{
		VirtualStorage:       ctx.String(paramVirtualStorage),
		RelativePath:         ctx.String(paramRelativePath),
		AuthoritativeStorage: ctx.String(paramAuthoritativeStorage),
	}); err != nil {
		return cli.Exit(fmt.Errorf("set authoritative storage: %w", err), 1)
	}

	return nil
}
