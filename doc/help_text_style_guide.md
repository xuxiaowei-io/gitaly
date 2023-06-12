# Help text style guide

The command-line interface (CLI) tools `gitaly` and `praefect` are the primary interfaces to Gitaly and Gitaly Cluster.
For this reason, it's important that help text is standardized and complete.

The [`urfave/cli` library](https://cli.urfave.org) is used for CLI tooling in this project. It provides a number of
options for developers to provide help text to users.

## Rules for options

To help users discover `gitaly` and `praefect` features through the CLI:

- Always provide a value for the `Description` field key. This field key provides a lot of space to describe the command
  or subcommand. At a minimum, provide a longer full sentence restatement of (but don't duplicate) the `Usage` field key
  value.
- Because of a [known issue](https://gitlab.com/gitlab-org/gitaly/-/issues/5350), always set the `HideHelpCommand`
  field key to `true`. Setting this hides the `COMMANDS:` section of the help text,
  which hides a generated list of available subcommands, if available.

  Instead, list subcommands in the value of the `Description` field key. For example:

  ```go
  Description: "This is a long description of all the things example command can do.\n\n" +

                "Provides the following subcommands:\n\n" +

                "- subcommand1\n\n" +
                "- subcommand2\n\n",
  ```

## Rules for command strings

When adding or updating `gitaly` and `praefect` CLI tools, use the following guidance for values for the different field
keys for commands and subcommands:

| Field key     | All lower case | Use period | Example key and value                                                                 |
|:--------------|:---------------|:-----------|:--------------------------------------------------------------------------------------|
| `Name`        | Yes            | No         | `Name: "example"`                                                                     |
| `Usage`       | No             | No         | `Usage: "Short description of example command"`                                       |
| `Description` | No             | Yes        | `Description: "This is a long description of all the things example command can do."` |

### Specific rules for `Description`

When providing values for the `Description` key:

- If referring to the subcommand itself, put the subcommand in backticks. For example:

  ```plaintext
  `subcommand` can be run at any time.
  ```

## Related topics

- [Voice and tone](https://design.gitlab.com/content/voice-and-tone) from GitLab Design System.
- [Language](https://docs.gitlab.com/ee/development/documentation/styleguide/index.html#language) from GitLab
  Documentation Style Guide.
