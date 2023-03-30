# Logging in Gitaly

Gitaly creates several kinds of log data.

## Go application logs

The main Gitaly process uses logrus to writes structured logs to
stdout. These logs use either the text or the JSON format of logrus,
depending on setting in the Gitaly config file.

The main Gitaly process writes log messages with global scope and
request scope. Request scoped messages can be recognized and filtered
by their correlation ID.

The main application logs include an access log for all requests.
Request-scoped errors are printed with the request correlation ID
attached.

Many Gitaly RPC's spawn Git processes which may write errors or
warnings to stderr. Gitaly will capture these stderr messages and
include them in its main log, tagged with the request correlation ID.

## Log files

In a few cases, Gitaly spawns process that cannot log to stderr
because stderr gets relayed to the end user, and we would risk leaking
information about the machine Gitaly runs on. One (the only?) example
is Git hooks. Because of this, the Gitaly config file also has a log
directory setting. Hooks that must log to a file will use that
directory.

Examples are:

- `gitlab-shell.log`
- `gitaly_hooks.log`
