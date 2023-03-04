package gitaly

import (
	"context"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitlab"
)

func newCheckCommand() *cli.Command {
	return &cli.Command{
		Name:      "check",
		Usage:     "verify internal API is accessible",
		ArgsUsage: "<configfile>",
		Action:    checkAction,
	}
}

func checkAction(ctx *cli.Context) error {
	logrus.SetLevel(logrus.ErrorLevel)

	if ctx.NArg() != 1 || ctx.Args().First() == "" {
		fmt.Fprintf(os.Stderr, "error: invalid argument(s)")
		cli.ShowSubcommandHelpAndExit(ctx, 2)
	}

	configPath := ctx.Args().First()

	fmt.Print("Checking GitLab API access: ")
	info, err := checkAPI(configPath)
	if err != nil {
		fmt.Println("FAILED")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Println("OK")
	fmt.Printf("GitLab version: %s\n", info.Version)
	fmt.Printf("GitLab revision: %s\n", info.Revision)
	fmt.Printf("GitLab Api version: %s\n", info.APIVersion)
	fmt.Printf("Redis reachable for GitLab: %t\n", info.RedisReachable)
	fmt.Println("OK")

	return nil
}

func checkAPI(configPath string) (*gitlab.CheckInfo, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: config_path %q: %w", configPath, err)
	}

	gitlabAPI, err := gitlab.NewHTTPClient(logrus.New(), cfg.Gitlab, cfg.TLS, prometheus.Config{})
	if err != nil {
		return nil, err
	}

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return hook.NewManager(cfg, config.NewLocator(cfg), gitCmdFactory, nil, gitlabAPI).Check(context.Background())
}
