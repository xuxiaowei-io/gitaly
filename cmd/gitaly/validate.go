package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/pelletier/go-toml/v2"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

type validationOutputError struct {
	Key     []string `json:"key,omitempty"`
	Message string   `json:"message,omitempty"`
}

type validationOutput struct {
	Errors []validationOutputError `json:"errors,omitempty"`
}

func execValidateConfiguration() {
	logrus.SetLevel(logrus.ErrorLevel)

	command := flag.NewFlagSet("validate-configuration", flag.ExitOnError)
	command.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %v validate-configuration < <configfile>\n", os.Args[0])
		command.PrintDefaults()
	}

	cfg, err := config.Load(os.Stdin)
	if err != nil {
		terr := &toml.DecodeError{}
		if errors.As(err, &terr) {
			row, column := terr.Position()
			jsonEncoded(os.Stdout, os.Stderr, "  ", validationOutput{Errors: []validationOutputError{{
				Key:     terr.Key(),
				Message: fmt.Sprintf("line %d column %d: %v", row, column, terr.Error()),
			}}})
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "processing input data: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		var terr config.ValidationErrors
		if errors.As(err, &terr) {
			out := validationOutput{}
			for _, err := range terr {
				out.Errors = append(out.Errors, validationOutputError{
					Key:     err.Key,
					Message: err.Message,
				})
			}
			jsonEncoded(os.Stdout, os.Stderr, "  ", out)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func jsonEncoded(outStream io.Writer, errStream io.Writer, indent string, val any) {
	encoder := json.NewEncoder(outStream)
	encoder.SetIndent("", indent)
	if err := encoder.Encode(val); err != nil {
		fmt.Fprintf(errStream, "writing results: %v\n", err)
	}
}
