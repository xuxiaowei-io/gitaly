package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
)

// WriteBlobConfig is the configuration used to write blobs into the repository.
type WriteBlobConfig struct {
	//  Path is used by git to decide which filters to run on the content.
	Path string
}

// WriteBlob writes a blob to the repository's object database and
// returns its object ID.
func (repo *Repo) WriteBlob(ctx context.Context, content io.Reader, cfg WriteBlobConfig) (git.ObjectID, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	options := []git.Option{
		git.Flag{Name: "--stdin"},
		git.Flag{Name: "-w"},
	}
	if cfg.Path != "" {
		options = append(options, git.ValueFlag{Name: "--path", Value: cfg.Path})
	}

	cmd, err := repo.Exec(ctx,
		git.Command{
			Name:  "hash-object",
			Flags: options,
		},
		git.WithStdin(content),
		git.WithStdout(stdout),
		git.WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", errorWithStderr(err, stderr.Bytes())
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("detecting object hash: %w", err)
	}

	oid, err := objectHash.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", err
	}

	return oid, nil
}
