package gitaly

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/pelletier/go-toml/v2"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/errors/cfgerror"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

const validationErrorCode = 2

type validationOutputError struct {
	Key     []string `json:"key,omitempty"`
	Message string   `json:"message,omitempty"`
}

type validationOutput struct {
	Errors []validationOutputError `json:"errors,omitempty"`
}

func jsonEncoded(outStream io.Writer, errStream io.Writer, indent string, val any) {
	encoder := json.NewEncoder(outStream)
	encoder.SetIndent("", indent)
	if err := encoder.Encode(val); err != nil {
		fmt.Fprintf(errStream, "writing results: %v\n", err)
	}
}

func newConfigurationCommand() *cli.Command {
	return &cli.Command{
		Name:  "configuration",
		Usage: "allows to run commands related to the configuration",
		Subcommands: []*cli.Command{
			{
				Name:   "validate",
				Usage:  "checks if provided on STDIN configuration is valid",
				Action: validateConfigurationAction,
			},
		},
	}
}

func validateConfigurationAction(ctx *cli.Context) error {
	logrus.SetLevel(logrus.ErrorLevel)

	cfg, err := config.Load(ctx.App.Reader)
	if err != nil {
		terr := &toml.DecodeError{}
		if errors.As(err, &terr) {
			row, column := terr.Position()
			jsonEncoded(ctx.App.Writer, ctx.App.ErrWriter, "  ", validationOutput{Errors: []validationOutputError{{
				Key:     terr.Key(),
				Message: fmt.Sprintf("line %d column %d: %v", row, column, terr.Error()),
			}}})
			return cli.Exit("", validationErrorCode)
		}

		return cli.Exit(fmt.Sprintf("processing input data: %v", err), 1)
	}

	out := validationOutput{}
	for _, err := range cfgerror.New().Append(cfg.ValidateV2()) {
		out.Errors = append(out.Errors, validationOutputError{
			Key:     err.Key,
			Message: err.Cause.Error(),
		})
	}

	if len(out.Errors) > 0 {
		jsonEncoded(ctx.App.Writer, ctx.App.ErrWriter, "  ", out)
		return cli.Exit("", validationErrorCode)
	}

	return nil
}
