package git

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
)

// WriteBlobConfig is the configuration used to write blobs into the repository.
type WriteBlobConfig struct {
	//  Path is used by git to decide which filters to run on the content.
	Path string
}

// WriteBlob writes a blob into the given repository.
func WriteBlob(ctx context.Context, repoExecutor RepositoryExecutor, content io.Reader, cfg WriteBlobConfig) (ObjectID, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	options := []Option{
		Flag{Name: "--stdin"},
		Flag{Name: "-w"},
	}
	if cfg.Path != "" {
		options = append(options, ValueFlag{Name: "--path", Value: cfg.Path})
	}

	cmd, err := repoExecutor.Exec(ctx,
		Command{
			Name:  "hash-object",
			Flags: options,
		},
		WithStdin(content),
		WithStdout(stdout),
		WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		if stderr.Len() == 0 {
			return "", err
		}
		return "", fmt.Errorf("%w, stderr: %q", err, stderr)
	}

	objectHash, err := repoExecutor.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("detecting object hash: %w", err)
	}

	oid, err := objectHash.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", err
	}

	return oid, nil
}
