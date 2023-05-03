package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
)

// MergeStage denotes the stage indicated by git-merge-tree(1) in the conflicting
// files information section. The man page for git-merge(1) holds more information
// regarding the values of the stages and what they indicate.
type MergeStage uint

const (
	// MergeStageAncestor denotes a conflicting file version from the common ancestor.
	MergeStageAncestor = MergeStage(1)
	// MergeStageOurs denotes a conflicting file version from our commit.
	MergeStageOurs = MergeStage(2)
	// MergeStageTheirs denotes a conflicting file version from their commit.
	MergeStageTheirs = MergeStage(3)
)

type mergeTreeConfig struct {
	allowUnrelatedHistories  bool
	conflictingFileNamesOnly bool
}

// MergeTreeOption is a function that sets a config in mergeTreeConfig.
type MergeTreeOption func(*mergeTreeConfig)

// WithAllowUnrelatedHistories lets MergeTree accept two commits that do not
// share a common ancestor.
func WithAllowUnrelatedHistories() MergeTreeOption {
	return func(options *mergeTreeConfig) {
		options.allowUnrelatedHistories = true
	}
}

// WithConflictingFileNamesOnly lets MergeTree only parse the conflicting filenames and
// not the additional information.
func WithConflictingFileNamesOnly() MergeTreeOption {
	return func(options *mergeTreeConfig) {
		options.conflictingFileNamesOnly = true
	}
}

// MergeTree calls git-merge-tree(1) with arguments, and parses the results from
// stdout.
func (repo *Repo) MergeTree(
	ctx context.Context,
	ours, theirs string,
	mergeTreeOptions ...MergeTreeOption,
) (git.ObjectID, error) {
	var config mergeTreeConfig

	for _, option := range mergeTreeOptions {
		option(&config)
	}

	flags := []git.Option{
		git.Flag{Name: "-z"},
		git.Flag{Name: "--write-tree"},
	}

	if config.allowUnrelatedHistories {
		flags = append(flags, git.Flag{Name: "--allow-unrelated-histories"})
	}

	if config.conflictingFileNamesOnly {
		flags = append(flags, git.Flag{Name: "--name-only"})
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("getting object hash %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = repo.ExecAndWait(
		ctx,
		git.Command{
			Name:  "merge-tree",
			Flags: flags,
			Args:  []string{ours, theirs},
		},
		git.WithStderr(&stderr),
		git.WithStdout(&stdout),
	)
	if err != nil {
		exitCode, success := command.ExitStatus(err)
		if !success {
			return "", errors.New("could not parse exit status of merge-tree(1)")
		}

		if exitCode > 1 {
			if text.ChompBytes(stderr.Bytes()) == "fatal: refusing to merge unrelated histories" {
				return "", &MergeTreeError{}
			}
			return "", fmt.Errorf("merge-tree: %w", err)
		}

		return parseMergeTreeError(objectHash, config, stdout.String())
	}

	oid, err := objectHash.FromHex(strings.Split(stdout.String(), "\x00")[0])
	if err != nil {
		return "", fmt.Errorf("hex to oid: %w", err)
	}

	return oid, nil
}

// parseMergeTreeError parses the output from git-merge-tree(1)'s stdout into
// a MergeTreeResult struct. The format for the output can be found at
// https://git-scm.com/docs/git-merge-tree#OUTPUT.
func parseMergeTreeError(objectHash git.ObjectHash, cfg mergeTreeConfig, output string) (git.ObjectID, error) {
	var mergeTreeError MergeTreeError

	oidAndConflictsBuf, infoMsg, ok := strings.Cut(output, "\x00\x00")
	if !ok {
		return "", fmt.Errorf("couldn't parse merge tree output: %s", output)
	}

	oidAndConflicts := strings.Split(oidAndConflictsBuf, "\x00")
	// When the output is of unexpected length
	if len(oidAndConflicts) < 2 {
		return "", errors.New("couldn't split oid and file info")
	}

	oid, err := objectHash.FromHex(oidAndConflicts[0])
	if err != nil {
		return "", fmt.Errorf("hex to oid: %w", err)
	}

	mergeTreeError.ConflictingFileInfo = make([]ConflictingFileInfo, len(oidAndConflicts[1:]))

	// From git-merge-tree(1), the information is of the format `<mode> <object> <stage> <filename>`
	// unless the `--name-only` option is used, in which case only the filename is output.
	// Note: that there is \t before the filename (https://gitlab.com/gitlab-org/git/blob/v2.40.0/builtin/merge-tree.c#L481)
	for i, infoLine := range oidAndConflicts[1:] {
		if cfg.conflictingFileNamesOnly {
			mergeTreeError.ConflictingFileInfo[i].FileName = infoLine
		} else {
			infoAndFilename := strings.Split(infoLine, "\t")
			if len(infoAndFilename) != 2 {
				return "", fmt.Errorf("parsing conflicting file info: %s", infoLine)
			}

			info := strings.Fields(infoAndFilename[0])
			if len(info) != 3 {
				return "", fmt.Errorf("parsing conflicting file info: %s", infoLine)
			}

			mergeTreeError.ConflictingFileInfo[i].OID, err = objectHash.FromHex(info[1])
			if err != nil {
				return "", fmt.Errorf("hex to oid: %w", err)
			}

			stage, err := strconv.Atoi(info[2])
			if err != nil {
				return "", fmt.Errorf("converting stage to int: %w", err)
			}

			if stage < 1 || stage > 3 {
				return "", fmt.Errorf("invalid value for stage: %d", stage)
			}

			mergeTreeError.ConflictingFileInfo[i].Stage = MergeStage(stage)
			mergeTreeError.ConflictingFileInfo[i].FileName = infoAndFilename[1]
		}
	}

	fields := strings.Split(infoMsg, "\x00")
	// The git output contains a null characted at the end, which creates a stray empty field.
	fields = fields[:len(fields)-1]

	for i := 0; i < len(fields); {
		c := ConflictInfoMessage{}

		numOfPaths, err := strconv.Atoi(fields[i])
		if err != nil {
			return "", fmt.Errorf("converting stage to int: %w", err)
		}

		if i+numOfPaths+2 > len(fields) {
			return "", fmt.Errorf("incorrect number of fields: %s", infoMsg)
		}

		c.Paths = fields[i+1 : i+numOfPaths+1]
		c.Type = fields[i+numOfPaths+1]
		c.Message = fields[i+numOfPaths+2]

		mergeTreeError.ConflictInfoMessage = append(mergeTreeError.ConflictInfoMessage, c)

		i = i + numOfPaths + 3
	}

	return oid, &mergeTreeError
}

// ConflictingFileInfo holds the conflicting file info output from git-merge-tree(1).
type ConflictingFileInfo struct {
	FileName string
	OID      git.ObjectID
	Stage    MergeStage
}

type ConflictInfoMessage struct {
	Paths   []string
	Type    string
	Message string
}

// MergeTreeError encapsulates any conflicting file info and messages that occur
// when a merge-tree(1) command fails.
type MergeTreeError struct {
	ConflictingFileInfo []ConflictingFileInfo
	ConflictInfoMessage []ConflictInfoMessage
}

// Error returns the error string for a conflict error.
func (c *MergeTreeError) Error() string {
	// TODO: for now, it's better that this error matches the git2go
	// error but once we deprecate the git2go code path in
	// merges, we can change this error to print out the conflicting files
	// and the InfoMessage.
	return "merge: there are conflicting files"
}
