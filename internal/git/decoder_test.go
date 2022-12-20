//go:build !gitaly_test_sha256

package git_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestShowRefDecoder(t *testing.T) {
	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	repoProto, repoPath := CreateRepository(t, ctx, cfg, CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   SeedGitLabTest,
		RelativePath:           "repo.git",
	})

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	expectedRefs, err := repo.GetReferences(ctx, "refs/")
	require.NoError(t, err)

	output := Exec(t, cfg, "-C", repoPath, "show-ref")
	stream := bytes.NewBuffer(output)

	d := NewShowRefDecoder(stream)

	var refs []Reference
	for {
		var ref Reference

		err := d.Decode(&ref)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		refs = append(refs, ref)
	}

	require.Equal(t, expectedRefs, refs)
}
