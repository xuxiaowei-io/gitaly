package praefect

import (
	"fmt"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/log"
)

func newListStoragesCommand() *cli.Command {
	return &cli.Command{
		Name:  "list-storages",
		Usage: "show virtual storages and their associated storages",
		Description: "This command lists virtual storages and their associated storages.\n" +
			"Passing a virtual-storage argument will print out the storage associated with\n" +
			"that particular virtual storage.\n",
		HideHelpCommand: true,
		Action:          listStoragesAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  paramVirtualStorage,
				Usage: "name of the virtual storage to list storages for",
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

func listStoragesAction(ctx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, ctx.String(configFlagName))
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(ctx.App.Writer)
	table.SetHeader([]string{"VIRTUAL_STORAGE", "NODE", "ADDRESS"})
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.SetTablePadding("\t") // pad with tabs
	table.SetNoWhiteSpace(true)

	if pickedVirtualStorage := ctx.String(paramVirtualStorage); pickedVirtualStorage != "" {
		for _, virtualStorage := range conf.VirtualStorages {
			if virtualStorage.Name != pickedVirtualStorage {
				continue
			}

			for _, node := range virtualStorage.Nodes {
				table.Append([]string{virtualStorage.Name, node.Storage, node.Address})
			}

			table.Render()

			return nil
		}

		fmt.Fprintf(ctx.App.Writer, "No virtual storages named %s.\n", pickedVirtualStorage)

		return nil
	}

	for _, virtualStorage := range conf.VirtualStorages {
		for _, node := range virtualStorage.Nodes {
			table.Append([]string{virtualStorage.Name, node.Storage, node.Address})
		}
	}

	table.Render()

	return nil
}
