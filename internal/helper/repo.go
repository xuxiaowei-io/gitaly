package helper

import "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"

// RepoPathEqual compares if two repositories are in the same location
func RepoPathEqual(a, b storage.Repository) bool {
	return a.GetStorageName() == b.GetStorageName() &&
		a.GetRelativePath() == b.GetRelativePath()
}
