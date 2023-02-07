package gitattributes

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestCheckAttrCmd_Check(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// Until https://gitlab.com/groups/gitlab-org/-/epics/9006 is completed
	// we rely on info/attributes.
	infoPath := filepath.Join(repoPath, "info")
	require.NoError(t, os.MkdirAll(infoPath, perm.SharedDir))
	attrPath := filepath.Join(infoPath, "attributes")

	for _, tc := range []struct {
		desc         string
		attrContent  string
		path         string
		expectedAttr Attributes
	}{
		{
			desc:         "no attributes",
			path:         "README.md",
			expectedAttr: Attributes{},
		},
		{
			desc:         "attribute on other file",
			attrContent:  "*.go foo",
			path:         "README.md",
			expectedAttr: Attributes{},
		},
		{
			desc:         "comments are ignored",
			attrContent:  "# *.md -foo",
			path:         "README.md",
			expectedAttr: Attributes{},
		},
		{
			desc:         "single attribute",
			attrContent:  "*.md foo",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "double star pattern",
			attrContent:  "doc/** foo",
			path:         "doc/development/readme.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "comment and single attribute",
			attrContent:  "# these are the gitattributes\n*.md foo",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "quoted pattern",
			attrContent:  `"*.\155\144" foo`,
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:        "multiple attributes",
			attrContent: "*.md foo bar",
			path:        "README.md",
			expectedAttr: Attributes{
				Attribute{Name: "foo", State: "set"},
				Attribute{Name: "bar", State: "set"},
			},
		},
		{
			desc:        "multiple attributes on multiple lines",
			attrContent: "*.md foo\n*.md bar",
			path:        "README.md",
			expectedAttr: Attributes{
				Attribute{Name: "foo", State: "set"},
				Attribute{Name: "bar", State: "set"},
			},
		},
		{
			desc:         "attributes on multiple files",
			attrContent:  "*.md foo\n*.go bar",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "set"}},
		},
		{
			desc:         "attributes with state",
			attrContent:  "*.md foo=bar",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "bar"}},
		},
		{
			desc:         "attributes with unset",
			attrContent:  "*.md -foo",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "unset"}},
		},
		{
			desc:         "attributes with unset overriding",
			attrContent:  "*.md foo\ndoc/*.md -foo",
			path:         "doc/README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "unset"}},
		},
		{
			desc:         "attributes with state overridden",
			attrContent:  "*.md foo=bar\n*.md foo=buzz",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "buzz"}},
		},
		{
			desc:         "attributes overridden for subdir",
			attrContent:  "*.md foo=bar\ndoc/*.md foo=buzz",
			path:         "doc/README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "buzz"}},
		},
		{
			desc:         "attributes overridden for other subdir",
			attrContent:  "*.md foo=bar\ndoc/*.md foo=buzz",
			path:         "README.md",
			expectedAttr: Attributes{Attribute{Name: "foo", State: "bar"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t, os.WriteFile(attrPath, []byte(tc.attrContent), perm.SharedFile))

			checkCmd, finish, err := CheckAttr(ctx, repo, []string{"foo", "bar"})
			require.NoError(t, err)
			t.Cleanup(finish)

			actual, err := checkCmd.Check(tc.path)
			require.NoError(t, err)
			require.Equal(t, tc.expectedAttr, actual)
		})
	}
}
