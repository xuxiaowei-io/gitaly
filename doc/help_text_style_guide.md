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
| `Usage`       | Yes            | No         | `Usage: "short description of example command"`                                       |
| `Description` | No             | Yes        | `Description: "This is a long description of all the things example command can do."` |

### Specific rules for `Usage`

When providing values for the `Usage` field key:

- Don't mention Gitaly or Praefect explicitly. We imply in the help text the tool we're referring to.

  - Do:

    ```plaintext
    start the daemon
    ```

  - Don't:

    ```plaintext
    start the Gitaly daemon
    ```

- Always start with a verb and always write the string from the user's perspective, not the tool's perspective:

  - Do:

    ```plaintext
    run the process
    ```

  - Don't:

    ```plaintext
    runs the process
    ```

  - Don't:

    ```plaintext
    the process runs
    ```

### Specific rules for `Description`

When providing values for the `Description` field key:

- If referring to subcommands or flags, put the subcommand or flag in backticks. For example:

  ```plaintext
  `subcommand` can be run at any time with the `flag` flag.
  ```

- Can mention Gitaly, Praefect, or Gitaly Cluster. For example:

  ```plaintext
  Runs all of the processes for the Gitaly Cluster.
  ```

If the command or subcommand requires more than just flags, add an example of invoking the command. For example:

```go
Description: "The subcommand accepts a file on stdin.\n\n" +

             "Example: `command subcommand < file`." +
```

## Related topics

- [Voice and tone](https://design.gitlab.com/content/voice-and-tone) from GitLab Design System.
- [Language](https://docs.gitlab.com/ee/development/documentation/styleguide/index.html#language) from GitLab
  Documentation Style Guide.
- [Command Line Interface Guidelines](https://clig.dev).
