package localrepo

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// WriteBlobConfig is the configuration used to write blobs into the repository.
type WriteBlobConfig = git.WriteBlobConfig

// WriteBlob writes a blob to the repository's object database and
// returns its object ID.
func (repo *Repo) WriteBlob(ctx context.Context, content io.Reader, cfg WriteBlobConfig) (git.ObjectID, error) {
	return git.WriteBlob(ctx, repo, content, cfg)
}
