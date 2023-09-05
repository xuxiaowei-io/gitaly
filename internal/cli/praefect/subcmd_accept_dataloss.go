package praefect

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func newAcceptDatalossCommand() *cli.Command {
	return &cli.Command{
		Name:  "accept-dataloss",
		Usage: "accept potential data loss in a repository",
		Description: `Set a new authoritative replica of a repository and enable the repository for writing again.

The replica of the repository on the specified physical storage is set as authoritative. Replications to other physical
storages that contain replicas of the repository are scheduled to make them consistent with the replica on the new
authoritative physical storage.

Example: praefect --config praefect.config.toml accept-dataloss --virtual-storage default --repository <relative_path_on_the_virtual_storage> --authoritative-storage <physical_storage_1>`,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     paramVirtualStorage,
				Usage:    "name of the repository's virtual storage",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramRelativePath,
				Usage:    "relative path on the virtual storage of the repository to accept data loss for",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramAuthoritativeStorage,
				Usage:    "physical storage containing the replica of the repository to set as authoritative",
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
	conf, err := readConfig(ctx.String(configFlagName))
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
