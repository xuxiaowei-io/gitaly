package praefect

import (
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const verifyCmdName = "verify"

func newVerifyCommand() *cli.Command {
	return &cli.Command{
		Name:  verifyCmdName,
		Usage: "marks a discrete repository, or repositories of a storage, or repositories of a virtual storage to be verified",
		Description: "The command marks a single repository if 'repository-id' flag is provided or a batch of\n" +
			"repositories that belong to a particular storage or virtual storage to be checked they exist.\n" +
			"The repository existence is confirmed if repository exists on the disk. That verification operation\n" +
			"runs in background and executes verification asynchronously.",
		HideHelpCommand: true,
		Action:          verifyAction,
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "repository-id",
				Usage: "the id of the repository to verify",
			},
			&cli.StringFlag{
				Name:  paramVirtualStorage,
				Usage: "the virtual storage to verify",
			},
			&cli.StringFlag{
				Name:  "storage",
				Usage: "the storage to verify",
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

func verifyAction(appCtx *cli.Context) error {
	logger := log.Default()
	conf, err := getConfig(logger, appCtx.String(configFlagName))
	if err != nil {
		return err
	}

	repositoryID := appCtx.Int64("repository-id")
	virtualStorage := appCtx.String(paramVirtualStorage)
	storage := appCtx.String("storage")

	var request gitalypb.MarkUnverifiedRequest
	switch {
	case repositoryID != 0:
		if virtualStorage != "" || storage != "" {
			return errors.New("virtual storage and storage can't be provided with a repository ID")
		}

		request.Selector = &gitalypb.MarkUnverifiedRequest_RepositoryId{RepositoryId: repositoryID}
	case storage != "":
		if virtualStorage == "" {
			return errors.New("virtual storage must be passed with storage")
		}

		request.Selector = &gitalypb.MarkUnverifiedRequest_Storage_{
			Storage: &gitalypb.MarkUnverifiedRequest_Storage{
				VirtualStorage: virtualStorage,
				Storage:        storage,
			},
		}
	case virtualStorage != "":
		request.Selector = &gitalypb.MarkUnverifiedRequest_VirtualStorage{VirtualStorage: virtualStorage}
	default:
		return errors.New("(repository id), (virtual storage) or (virtual storage, storage) required")
	}

	nodeAddr, err := getNodeAddress(conf)
	if err != nil {
		return fmt.Errorf("get node address: %w", err)
	}

	ctx := appCtx.Context
	conn, err := subCmdDial(ctx, nodeAddr, conf.Auth.Token, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	response, err := gitalypb.NewPraefectInfoServiceClient(conn).MarkUnverified(ctx, &request)
	if err != nil {
		return fmt.Errorf("verify replicas: %w", err)
	}

	fmt.Fprintf(appCtx.App.Writer, "%d replicas marked unverified\n", response.GetReplicasMarked())

	return nil
}
