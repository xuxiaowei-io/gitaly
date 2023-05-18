package praefect

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const (
	setReplicationFactorCmdName = "set-replication-factor"
	paramReplicationFactor      = "replication-factor"
)

func newSetReplicationFactorCommand() *cli.Command {
	return &cli.Command{
		Name:  setReplicationFactorCmdName,
		Usage: "set a replication factor for a repository",
		Description: "The set-replication-factor subcommand changes the replication factor for an existing repository.\n" +
			"The subcommand assigns or unassigns host nodes from the repository to meet the set replication factor.\n" +
			"The subcommand returns an error if you try to set a replication factor:\n\n" +
			"- More than the storage node count in the virtual storage.\n" +
			"- Less than one.\n\n" +
			"The primary node isn't unassigned because:\n\n" +
			"- It needs a copy of the repository to accept writes.\n" +
			"- It is the first storage that gets assigned when setting a replication factor for a repository.\n\n" +
			"Assignments of unconfigured storages are ignored. This might cause the actual replication factor\n" +
			"to be higher than required if the replication factor is set during an upgrade of a Praefect node\n" +
			"that does not yet know about a new node. Because assignments of unconfigured storages are ignored, the\n" +
			"replication factor of repositories assigned to a storage node removed from the cluster is effectively\n" +
			"decreased.",
		HideHelpCommand: true,
		Action:          setReplicationFactorAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     paramVirtualStorage,
				Usage:    "name of the repository's virtual storage",
				Required: true,
			},
			&cli.StringFlag{
				Name:     paramRelativePath,
				Usage:    "repository to set the replication factor for",
				Required: true,
			},
			&cli.UintFlag{
				Name:     paramReplicationFactor,
				Usage:    "desired replication factor",
				Required: true,
			},
		},
		Before: func(ctx *cli.Context) error {
			if ctx.Args().Present() {
				_ = cli.ShowSubcommandHelp(ctx)
				return cli.Exit(unexpectedPositionalArgsError{Command: ctx.Command.Name}, 1)
			}
			return nil
		},
	}
}

func setReplicationFactorAction(appCtx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	virtualStorage := appCtx.String(paramVirtualStorage)
	relativePath := appCtx.String(paramRelativePath)
	replicationFactor := appCtx.Uint(paramReplicationFactor)

	nodeAddr, err := getNodeAddress(conf)
	if err != nil {
		return err
	}

	ctx := appCtx.Context
	conn, err := subCmdDial(ctx, nodeAddr, conf.Auth.Token, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("error dialing: %w", err)
	}
	defer conn.Close()

	client := gitalypb.NewPraefectInfoServiceClient(conn)
	resp, err := client.SetReplicationFactor(ctx, &gitalypb.SetReplicationFactorRequest{
		VirtualStorage:    virtualStorage,
		RelativePath:      relativePath,
		ReplicationFactor: int32(replicationFactor),
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(appCtx.App.Writer, "current assignments: %v\n", strings.Join(resp.Storages, ", "))

	return nil
}
