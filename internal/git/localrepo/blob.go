package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
)

// WriteBlob writes a blob to the repository's object database and
// returns its object ID. Path is used by git to decide which filters to
// run on the content.
func (repo *Repo) WriteBlob(ctx context.Context, path string, content io.Reader) (git.ObjectID, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd, err := repo.Exec(ctx,
		git.Command{
			Name: "hash-object",
			Flags: []git.Option{
				git.ValueFlag{Name: "--path", Value: path},
				git.Flag{Name: "--stdin"},
				git.Flag{Name: "-w"},
			},
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
