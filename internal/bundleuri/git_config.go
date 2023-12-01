package bundleuri

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
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

// UploadPackGitConfig return a slice of git.ConfigPairs you can inject into the
// call to git-upload-pack(1) to advertise the available bundle to the client
// who clones/fetches from the repository.
// In case no backups could be found or something else goes wrong, an empty
// slice is returned without error.
func UploadPackGitConfig(
	ctx context.Context,
	backupLocator backup.Locator,
	backupSink backup.Sink,
	repo storage.Repository,
) []git.ConfigPair {
	if backupLocator == nil || backupSink == nil || featureflag.BundleURI.IsDisabled(ctx) {
		return []git.ConfigPair{}
	}

	theBackup, err := backupLocator.FindLatest(ctx, repo)
	if err != nil {
		return []git.ConfigPair{}
	}

	for _, step := range theBackup.Steps {
		// Skip non-existing & incremental backups
		if len(step.BundlePath) == 0 || len(step.PreviousRefPath) > 0 {
			continue
		}

		uri, err := backupSink.SignedURL(ctx, step.BundlePath, 10*time.Minute)
		if err != nil {
			return []git.ConfigPair{}
		}

		log.AddFields(ctx, log.Fields{"bundle_uri": true})

		return []git.ConfigPair{
			{
				Key:   "uploadpack.advertiseBundleURIs",
				Value: "true",
			},
			{
				Key:   "bundle.version",
				Value: "1",
			},
			{
				Key:   "bundle.mode",
				Value: "any",
			},
			{
				Key:   "bundle.some.uri",
				Value: uri,
			},
		}
	}

	return []git.ConfigPair{}
}
