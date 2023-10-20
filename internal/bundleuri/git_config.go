package bundleuri

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

// InfoRefsGitConfig return a slice of git.ConfigPairs you can inject into the
// call to git-upload-pack(1) --advertise-refs, to advertise the use of
// bundle-URI to the client who clones/fetches from the repository.
func InfoRefsGitConfig(ctx context.Context) []git.ConfigPair {
	if featureflag.BundleURI.IsDisabled(ctx) {
		return []git.ConfigPair{}
	}

	return []git.ConfigPair{
		{
			Key:   "uploadpack.advertiseBundleURIs",
			Value: "true",
		},
	}
}

