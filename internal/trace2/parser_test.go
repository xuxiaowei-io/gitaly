package trace2

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestParser_Parse(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc          string
		events        string
		expectedErr   string
		expectedTrace string
	}{
		{
			desc:   "empty events",
			events: "",
		},
		{
			desc:        "invalid json format",
			events:      "hello",
			expectedErr: "decode event: invalid character 'h' looking for beginning of value",
		},
		{
			desc: "incomplete events",
			events: `
{"event":"version","thread":"main","time":"2023-02-22T07:24:36.291735Z","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1"}
{"event":"start","thread":"main"}
{""
`,
			expectedErr: "decode event: unexpected EOF",
		},
		{
			desc:        "mismatched event data type",
			events:      `{"event":"start","thread":"main","t_abs": "1234"}`,
			expectedErr: "decode event: json: cannot unmarshal string into Go struct field jsonEvent.t_abs of type float64",
		},
		{
			desc:        "invalid time",
			events:      `{"event":"start","thread":"main","time": "1234"}`,
			expectedErr: `process event: parse event time: parsing time "1234" as "2006-01-02T15:04:05.000000Z": cannot parse "" as "-"`,
		},
		{
			desc:        "mismatched data value format",
			events:      `{"event":"data","thread":"main","key": "hello", "value": 123}`,
			expectedErr: `process event: mismatched data value: json: cannot unmarshal number into Go value of type string`,
		},
		{
			desc:   "sampled git status events",
			events: string(testhelper.MustReadFile(t, "testdata/git-status.event")),
			expectedTrace: `
08:10:10.668546 | 08:10:10.687877 | main | root (code="0")
08:10:10.668546 | 08:10:10.668546 | main | .version
08:10:10.754608 | 08:10:10.754608 | main | .start (argv="git status")
08:10:10.754608 | 08:10:10.754608 | main | .def_repo
08:10:10.754608 | 08:10:10.754608 | main | .cmd_name
08:10:10.754608 | 08:10:10.755102 | main | .index:do_read_index (msg=".git/index")
08:10:10.754608 | 08:10:10.754732 | main | ..cache_tree:read
08:10:10.758297 | 08:10:10.758297 | main | ..data:index:read/version (data="2")
08:10:10.758318 | 08:10:10.758318 | main | ..data:index:read/cache_nr (data="1585")
08:10:10.755102 | 08:10:10.759927 | main | .progress:Refresh index
08:10:10.755102 | 08:10:10.759845 | main | ..index:preload
08:10:10.759845 | 08:10:10.759862 | main | ..index:refresh
08:10:10.763258 | 08:10:10.763258 | main | ..data:progress:total_objects (data="1585")
08:10:10.759927 | 08:10:10.759999 | main | .status:worktrees
08:10:10.759927 | 08:10:10.759932 | main | ..diff:setup
08:10:10.759932 | 08:10:10.759936 | main | ..diff:write back to queue
08:10:10.759999 | 08:10:10.761522 | main | .status:index
08:10:10.759999 | 08:10:10.760078 | main | ..unpack_trees:unpack_trees
08:10:10.760078 | 08:10:10.760081 | main | ..diff:setup
08:10:10.760081 | 08:10:10.760084 | main | ..diff:write back to queue
08:10:10.761522 | 08:10:10.769788 | main | .status:untracked
08:10:10.761522 | 08:10:10.769753 | main | ..dir:read_directory
08:10:10.773512 | 08:10:10.773512 | main | .data:status:count/changed (data="0")
08:10:10.773518 | 08:10:10.773518 | main | .data:status:count/untracked (data="1")
08:10:10.773522 | 08:10:10.773522 | main | .data:status:count/ignored (data="0")
08:10:10.773522 | 08:10:10.773727 | main | .status:print
08:10:10.773741 | 08:10:10.773741 | main | .exit
08:10:10.773750 | 08:10:10.773750 | main | .data_json:traverse_trees:statistics (data="{\"traverse_trees_count\":1,\"traverse_trees_max_depth\":1}")
`,
		},
		{
			desc:   "sampled git fetch events",
			events: string(testhelper.MustReadFile(t, "testdata/git-fetch.event")),
			expectedTrace: `
07:24:36.291735 | 07:24:40.554407 | main | root (code="0")
07:24:36.291735 | 07:24:36.291735 | main | .version
07:24:36.293932 | 07:24:36.293932 | main | .start (argv="git fetch origin master")
07:24:36.293932 | 07:24:36.293932 | main | .def_repo
07:24:36.293932 | 07:24:36.293932 | main | .cmd_name
07:24:36.293932 | 07:24:36.294119 | main | .index:do_read_index (msg=".git/index")
07:24:36.293932 | 07:24:36.293975 | main | ..cache_tree:read
07:24:36.294718 | 07:24:36.294718 | main | ..data:index:read/version (data="2")
07:24:36.294725 | 07:24:36.294725 | main | ..data:index:read/cache_nr (data="1589")
07:24:36.294119 | 07:24:36.330739 | main | .fetch:remote_refs (code="0")
07:24:36.294119 | 07:24:40.128447 | main (child 0) | ..child_start (argv="ssh -o SendEnv=GIT_PROTOCOL git@gitlab.com git-upload-pack 'gitlab-org/gitaly.git'")
07:24:39.314775 | 07:24:39.314775 | main (child 0) | ...data:transfer:negotiated-version (data="2")
07:24:40.128447 | 07:24:36.324327 | main (child 1) | ..child_start (argv="git rev-list --objects --stdin --not --all --quiet --alternate-refs" code="0")
07:24:40.148998 | 07:24:40.148998 | main (child 1) | ...version
07:24:36.295636 | 07:24:36.295636 | main (child 1) | ...start (argv="git rev-list --objects --stdin --not --all --quiet --alternate-refs")
07:24:36.295636 | 07:24:36.295636 | main (child 1) | ...def_repo
07:24:36.295636 | 07:24:36.295636 | main (child 1) | ...cmd_name
07:24:36.324300 | 07:24:36.324300 | main (child 1) | ...exit
07:24:36.330739 | 07:24:36.331472 | main | .fetch:consume_refs
07:24:36.291735 | 07:24:36.291820 | main | .submodule:parallel/fetch (msg="max:1")
07:24:36.291820 | 07:24:36.297895 | main (child 2) | .child_start (argv="git maintenance run --auto --no-quiet" code="0")
07:24:40.548630 | 07:24:40.548630 | main (child 2) | ..version
07:24:36.294627 | 07:24:36.294627 | main (child 2) | ..start (argv="git maintenance run --auto --no-quiet")
07:24:36.294627 | 07:24:36.294627 | main (child 2) | ..def_repo
07:24:36.294627 | 07:24:36.294627 | main (child 2) | ..cmd_name
07:24:36.297878 | 07:24:36.297878 | main (child 2) | ..exit
07:24:40.554393 | 07:24:40.554393 | main | .exit
`,
		},
		{
			desc:   "sampled git commit events",
			events: string(testhelper.MustReadFile(t, "testdata/git-commit.event")),
			expectedTrace: `
11:26:37.174893 | 11:26:38.677971 | main | root (code="0")
11:26:37.174893 | 11:26:37.174893 | main | .version
11:26:37.180753 | 11:26:37.180753 | main | .start (argv="git commit --amend")
11:26:37.180753 | 11:26:37.180753 | main | .def_repo
11:26:37.180753 | 11:26:37.180753 | main | .cmd_name
11:26:37.180753 | 11:26:37.181072 | main | .index:do_read_index (msg=".git/index")
11:26:37.180753 | 11:26:37.180832 | main | ..cache_tree:read
11:26:37.185091 | 11:26:37.185091 | main | ..data:index:read/version (data="2")
11:26:37.185105 | 11:26:37.185105 | main | ..data:index:read/cache_nr (data="1590")
11:26:37.181072 | 11:26:37.187270 | main | .index:preload
11:26:37.191313 | 11:26:37.191313 | main | ..data:index:preload/sum_lstat (data="1590")
11:26:37.187270 | 11:26:37.187432 | main | .index:preload
11:26:37.191812 | 11:26:37.191812 | main | ..data:index:preload/sum_lstat (data="0")
11:26:37.187432 | 11:26:37.187492 | main | .index:refresh
11:26:37.191874 | 11:26:37.191874 | main | ..data:index:refresh/sum_lstat (data="0")
11:26:37.191906 | 11:26:37.191906 | main | ..data:index:refresh/sum_scan (data="0")
11:26:37.187492 | 11:26:37.194463 | main | .cache_tree:update
11:26:37.194463 | 11:26:37.194919 | main | .index:do_write_index (msg="/gitaly/.git/index.lock")
11:26:37.194463 | 11:26:37.194533 | main | ..cache_tree:write
11:26:37.199369 | 11:26:37.199369 | main | ..data:index:write/version (data="2")
11:26:37.199382 | 11:26:37.199382 | main | ..data:index:write/cache_nr (data="1590")
11:26:37.194919 | 11:26:37.194991 | main | .status:worktrees
11:26:37.194919 | 11:26:37.194927 | main | ..diff:setup
11:26:37.194927 | 11:26:37.194933 | main | ..diff:write back to queue
11:26:37.194991 | 11:26:37.195415 | main | .status:index
11:26:37.194991 | 11:26:37.195147 | main | ..unpack_trees:unpack_trees
11:26:37.195147 | 11:26:37.195154 | main | ..diff:setup
11:26:37.195154 | 11:26:37.195162 | main | ..diff:write back to queue
11:26:37.195415 | 11:26:37.208155 | main | .status:untracked
11:26:37.195415 | 11:26:37.208106 | main | ..dir:read_directory
11:26:37.214048 | 11:26:37.214048 | main | .data:status:count/changed (data="5")
11:26:37.214055 | 11:26:37.214055 | main | .data:status:count/untracked (data="0")
11:26:37.214060 | 11:26:37.214060 | main | .data:status:count/ignored (data="0")
11:26:37.214060 | 11:26:37.214271 | main | .status:print
11:26:37.214271 | 11:26:37.214297 | main | .cache_tree:update
11:26:37.214297 | 11:26:37.176472 | main (child 0) | .child_start (argv="nvim /gitaly/.git/COMMIT_EDITMSG" code="128")
11:26:37.353241 | 11:26:37.353241 | main (child 0) | ..version
11:26:37.175671 | 11:26:37.175671 | main (child 0) | ..start (argv="git diff --no-color --no-ext-diff -U0 -- COMMIT_EDITMSG")
11:26:37.175671 | 11:26:37.175671 | main (child 0) | ..cmd_name
11:26:37.175671 | 11:26:37.175671 | main (child 0) | ..error (msg="this operation must be run in a work tree")
11:26:37.176465 | 11:26:37.176465 | main (child 0) | ..exit
11:26:38.624567 | 11:26:38.624567 | main | .version
11:26:37.175517 | 11:26:37.175517 | main | .start (argv="git branch --no-color --show-current")
11:26:37.175517 | 11:26:37.175517 | main | .def_repo
11:26:37.175517 | 11:26:37.175517 | main | .cmd_name
11:26:37.176095 | 11:26:37.176095 | main | .exit
11:26:37.174893 | 11:26:37.180102 | main (child 1) | .child_start (argv="git maintenance run --auto --no-quiet" code="0")
11:26:38.667832 | 11:26:38.667832 | main (child 1) | ..version
11:26:37.178802 | 11:26:37.178802 | main (child 1) | ..start (argv="git maintenance run --auto --no-quiet")
11:26:37.178802 | 11:26:37.178802 | main (child 1) | ..def_repo
11:26:37.178802 | 11:26:37.178802 | main (child 1) | ..cmd_name
11:26:37.180092 | 11:26:37.180092 | main (child 1) | ..exit
11:26:37.174893 | 11:26:37.174907 | main | .diff:setup
11:26:37.174907 | 11:26:37.174912 | main | .diff:write back to queue
11:26:38.677953 | 11:26:38.677953 | main | .exit
11:26:38.677965 | 11:26:38.677965 | main | .data_json:traverse_trees:statistics (data="{\"traverse_trees_count\":2,\"traverse_trees_max_depth\":2}")
`,
		},
		{
			desc:   "sampled git pack objects events",
			events: string(testhelper.MustReadFile(t, "testdata/git-pack-objects.event")),
			expectedTrace: `
12:05:04.840009 | 12:05:04.848504 | main | root (code="0")
12:05:04.840009 | 12:05:04.840009 | main | .version
12:05:04.842347 | 12:05:04.842347 | main | .start (argv="git pack-objects toon --compression=0")
12:05:04.842347 | 12:05:04.842347 | main | .def_repo
12:05:04.842347 | 12:05:04.842347 | main | .cmd_name
12:05:04.842347 | 12:05:04.843782 | main | .pack-objects:enumerate-objects
12:05:04.842347 | 12:05:04.843430 | main | ..progress:Enumerating objects
12:05:04.843782 | 12:05:04.843872 | main | .pack-objects:prepare-pack
12:05:04.843782 | 12:05:04.843857 | main | ..progress:Counting objects
12:05:04.843872 | 12:05:04.847874 | main | .pack-objects:write-pack-file
12:05:04.843872 | 12:05:04.847844 | main | ..progress:Writing objects
12:05:04.848460 | 12:05:04.848460 | main | ..data:pack-objects:write_pack_file/wrote (data="1")
12:05:04.848491 | 12:05:04.848491 | main | .data:fsync:fsync/writeout-only (data="2")
12:05:04.848498 | 12:05:04.848498 | main | .exit
`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			parser := &Parser{}
			trace, err := parser.Parse(testhelper.Context(t), strings.NewReader(tc.events))

			if tc.expectedErr != "" {
				// JSON doesn't export error creation. It's not feasible to compare
				// error directly. We can only compare by the stringified error
				require.Error(t, err)
				require.Equal(t, tc.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, strings.TrimSpace(tc.expectedTrace), trace.Inspect())
		})
	}
}
