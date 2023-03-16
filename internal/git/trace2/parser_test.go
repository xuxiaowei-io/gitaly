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
			expectedErr: "reading event: decoding event: invalid character 'h' looking for beginning of value",
		},
		{
			desc: "incomplete events",
			events: `
{"event":"version","thread":"main","time":"2023-02-22T07:24:36.291735Z","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1"}
{"event":"start","thread":"main"}
{""
`,
			expectedErr: "reading event: decoding event: unexpected EOF",
		},
		{
			desc:        "mismatched event data type",
			events:      `{"event":"start","thread":"main","t_abs": "1234"}`,
			expectedErr: "reading event: decoding event: json: cannot unmarshal string into Go struct field jsonEvent.t_abs of type float64",
		},
		{
			desc:        "invalid time",
			events:      `{"event":"start","thread":"main","time": "1234"}`,
			expectedErr: `processing event: parsing event time: parsing time "1234" as "2006-01-02T15:04:05.000000Z": cannot parse "" as "-"`,
		},
		{
			desc: "mismatched data value format",
			events: `
{"event":"start","thread":"main","time":"2023-02-22T07:24:36.291735Z"}
{"event":"data","thread":"main","key": "hello", "value": 123}
`,
			expectedErr: `processing event: mismatched data value: json: cannot unmarshal number into Go value of type string`,
		},
		{
			desc: "unexpected region_leave event",
			events: `
{"event":"version","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","time":"2023-02-22T12:05:04.840009Z","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1"}
{"event":"start","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"common-main.c","line":51,"t_abs":0.002338,"argv":["git","pack-objects","toon","--compression=0"]}
{"event":"def_repo","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"repository.c","line":136,"repo":1,"worktree":"/gitaly"}
{"event":"cmd_name","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"git.c","line":461,"name":"pack-objects","hierarchy":"pack-objects"}
{"event":"region_enter","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"builtin/pack-objects.c","line":4460,"repo":1,"nesting":1,"category":"pack-objects","label":"enumerate-objects"}
{"event":"region_enter","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"progress.c","line":268,"repo":1,"nesting":2,"category":"progress","label":"Enumerating objects"}
{"event":"region_leave","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"progress.c","line":346,"repo":1,"t_rel":0.001083,"nesting":2,"category":"progress","label":"Enumerating objects"}
{"event":"region_leave","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"builtin/pack-objects.c","line":4490,"repo":1,"t_rel":0.001435,"nesting":1,"category":"pack-objects","label":"enumerate-objects"}
{"event":"region_leave","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"builtin/pack-objects.c","line":4490,"repo":1,"t_rel":0.001435,"nesting":1,"category":"pack-objects","label":"enumerate-objects"}
{"event":"region_leave","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"builtin/pack-objects.c","line":4490,"repo":1,"t_rel":0.001435,"nesting":1,"category":"pack-objects","label":"enumerate-objects"}
{"event":"exit","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"git.c","line":721,"t_abs":0.008489,"code":0}
{"event":"atexit","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"trace2/tr2_tgt_event.c","line":204,"t_abs":0.008495,"code":0}
`,
			expectedErr: `processing event: unmatched leaving event`,
		},
		{
			desc: "unexpected child_exit event",
			events: `
{"event":"version","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","time":"2023-02-22T07:24:36.291735Z","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1"}
{"event":"start","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"common-main.c","line":51,"t_abs":0.002197,"argv":["git","fetch","origin","master"]}
{"event":"def_repo","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"repository.c","line":136,"repo":1,"worktree":"/gitaly"}
{"event":"cmd_name","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"git.c","line":461,"name":"fetch","hierarchy":"fetch"}
{"event":"region_enter","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"read-cache.c","line":2478,"repo":1,"nesting":1,"category":"index","label":"do_read_index","msg":".git/index"}
{"event":"region_enter","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"cache-tree.c","line":628,"repo":1,"nesting":2,"category":"cache_tree","label":"read"}
{"event":"region_leave","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"cache-tree.c","line":630,"repo":1,"t_rel":0.000043,"nesting":2,"category":"cache_tree","label":"read"}
{"event":"child_start","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"run-command.c","line":722,"child_id":0,"child_class":"transport/ssh","use_shell":false,"argv":["ssh","-o","SendEnv=GIT_PROTOCOL","git@gitlab.com","git-upload-pack 'gitlab-org/gitaly.git'"]}
{"event":"data","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"connect.c","line":167,"t_abs":3.023040,"t_rel":3.018289,"nesting":2,"category":"transfer","key":"negotiated-version","value":"2"}
{"event":"region_leave","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"builtin/fetch.c","line":1649,"repo":1,"t_rel":3.834328,"nesting":1,"category":"fetch","label":"remote_refs"}
{"event":"child_start","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"run-command.c","line":722,"child_id":1,"child_class":"?","use_shell":false,"argv":["git","rev-list","--objects","--stdin","--not","--all","--quiet","--alternate-refs"]}
{"event":"version","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515/20230222T072440.148843Z-Ha0f0bee5-P0000751a","thread":"main","time":"2023-02-22T07:24:40.148998Z","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1"}
{"event":"start","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515/20230222T072440.148843Z-Ha0f0bee5-P0000751a","thread":"main","file":"common-main.c","line":51,"t_abs":0.003901,"argv":["git","rev-list","--objects","--stdin","--not","--all","--quiet","--alternate-refs"]}
{"event":"def_repo","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515/20230222T072440.148843Z-Ha0f0bee5-P0000751a","thread":"main","file":"repository.c","line":136,"repo":1,"worktree":"/gitaly"}
{"event":"cmd_name","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515/20230222T072440.148843Z-Ha0f0bee5-P0000751a","thread":"main","file":"git.c","line":461,"name":"rev-list","hierarchy":"fetch/rev-list"}
{"event":"exit","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515/20230222T072440.148843Z-Ha0f0bee5-P0000751a","thread":"main","file":"git.c","line":721,"t_abs":0.032566,"code":0}
{"event":"atexit","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515/20230222T072440.148843Z-Ha0f0bee5-P0000751a","thread":"main","file":"trace2/tr2_tgt_event.c","line":204,"t_abs":0.032593,"code":0}
{"event":"child_exit","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"run-command.c","line":979,"child_id":1,"pid":29978,"code":0,"t_rel":0.036620}
{"event":"child_exit","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"run-command.c","line":979,"child_id":0,"pid":29974,"code":0,"t_rel":4.245077}
{"event":"region_enter","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"builtin/fetch.c","line":1353,"repo":1,"nesting":1,"category":"fetch","label":"consume_refs"}
{"event":"region_leave","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"builtin/fetch.c","line":1357,"repo":1,"t_rel":0.000733,"nesting":1,"category":"fetch","label":"consume_refs"}
{"event":"child_exit","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"run-command.c","line":979,"child_id":0,"pid":29974,"code":0,"t_rel":4.245077}
{"event":"exit","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"git.c","line":721,"t_abs":4.262658,"code":0}
{"event":"atexit","sid":"1234/20230222T072436.291562Z-Ha0f0bee5-P00007515","thread":"main","file":"trace2/tr2_tgt_event.c","line":204,"t_abs":4.262672,"code":0}
`,
			expectedErr: `processing event: unmatched leaving event`,
		},
		{
			desc: "incomplete events without enough leaving events",
			events: `
{"event":"version","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","time":"2023-02-22T12:05:04.840009Z","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1"}
{"event":"start","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"common-main.c","line":51,"t_abs":0.002338,"argv":["git","pack-objects","toon","--compression=0"]}
{"event":"def_repo","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"repository.c","line":136,"repo":1,"worktree":"/gitaly"}
{"event":"cmd_name","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"git.c","line":461,"name":"pack-objects","hierarchy":"pack-objects"}
{"event":"region_enter","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"builtin/pack-objects.c","line":4460,"repo":1,"nesting":1,"category":"pack-objects","label":"enumerate-objects"}
{"event":"region_enter","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"progress.c","line":268,"repo":1,"nesting":2,"category":"progress","label":"Enumerating objects"}
{"event":"region_leave","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"progress.c","line":346,"repo":1,"t_rel":0.001083,"nesting":2,"category":"progress","label":"Enumerating objects"}
`,
			expectedTrace: `
2023-02-22T12:05:04Z | 0001-01-01T00:00:00Z |   | main | root
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .version
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .start (argv="git pack-objects toon --compression=0")
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .def_repo
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .pack-objects:enumerate-objects
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | ..progress:Enumerating objects
`,
		},
		{
			desc: "initial time is missing",
			events: `
{"event":"version","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1"}
{"event":"start","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"common-main.c","line":51,"t_abs":0.002338,"argv":["git","pack-objects","toon","--compression=0"]}
{"event":"def_repo","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"repository.c","line":136,"repo":1,"worktree":"/gitaly"}
{"event":"cmd_name","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"git.c","line":461,"name":"pack-objects","hierarchy":"pack-objects"}
{"event":"region_enter","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"builtin/pack-objects.c","line":4460,"repo":1,"nesting":1,"category":"pack-objects","label":"enumerate-objects"}
{"event":"region_enter","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"progress.c","line":268,"repo":1,"nesting":2,"category":"progress","label":"Enumerating objects"}
{"event":"region_leave","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"progress.c","line":346,"repo":1,"t_rel":0.001083,"nesting":2,"category":"progress","label":"Enumerating objects"}
{"event":"region_leave","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"builtin/pack-objects.c","line":4490,"repo":1,"t_rel":0.001435,"nesting":1,"category":"pack-objects","label":"enumerate-objects"}
{"event":"exit","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"git.c","line":721,"t_abs":0.008489,"code":0}
{"event":"atexit","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"trace2/tr2_tgt_event.c","line":204,"t_abs":0.008495,"code":0}
`,
			expectedErr: `processing event: parsing event time: initial time is missing`,
		},
		{
			desc: "initial time is missing",
			events: `
{"event":"version","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"common-main.c","line":50,"evt":"3","exe":"2.39.1", "t_abs":0.002338}
{"event":"start","sid":"1234/20230222T120504.839855Z-Ha0f0bee5-P0000ccc6","thread":"main","file":"common-main.c","line":51,"t_abs":0.002338,"argv":["git","pack-objects","toon","--compression=0"]}
`,
			expectedErr: `processing event: parsing event time: initial time is missing`,
		},
		{
			desc:   "sampled git status events",
			events: string(testhelper.MustReadFile(t, "testdata/git-status.event")),
			expectedTrace: `
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | root (code="0")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .version
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .start (argv="git status")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .def_repo
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .index:do_read_index (msg=".git/index")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..cache_tree:read
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..data:index:read/version (data="2")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..data:index:read/cache_nr (data="1585")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .progress:Refresh index
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..index:preload
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..index:refresh
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..data:progress:total_objects (data="1585")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .status:worktrees
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..diff:setup
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..diff:write back to queue
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .status:index
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..unpack_trees:unpack_trees
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..diff:setup
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..diff:write back to queue
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .status:untracked
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | ..dir:read_directory
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .data:status:count/changed (data="0")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .data:status:count/untracked (data="1")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .data:status:count/ignored (data="0")
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .status:print
2023-02-21T08:10:10Z | 2023-02-21T08:10:10Z |   | main | .data_json:traverse_trees:statistics (data="{\"traverse_trees_count\":1,\"traverse_trees_max_depth\":1}")
`,
		},
		{
			desc:   "sampled git fetch events",
			events: string(testhelper.MustReadFile(t, "testdata/git-fetch.event")),
			expectedTrace: `
2023-02-22T07:24:36Z | 2023-02-22T07:24:40Z |   | main | root (code="0")
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z |   | main | .version
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z |   | main | .start (argv="git fetch origin master")
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z |   | main | .def_repo
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z |   | main | .index:do_read_index (msg=".git/index")
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z |   | main | ..cache_tree:read
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z |   | main | ..data:index:read/version (data="2")
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z |   | main | ..data:index:read/cache_nr (data="1589")
2023-02-22T07:24:36Z | 2023-02-22T07:24:40Z |   | main | .fetch:remote_refs (code="0")
2023-02-22T07:24:36Z | 2023-02-22T07:24:39Z | 0 | main | ..child_start (argv="ssh -o SendEnv=GIT_PROTOCOL git@gitlab.com git-upload-pack 'gitlab-org/gitaly.git'")
2023-02-22T07:24:39Z | 2023-02-22T07:24:39Z | 0 | main | ...data:transfer:negotiated-version (data="2")
2023-02-22T07:24:39Z | 2023-02-22T07:24:39Z | 1 | main | ..child_start (argv="git rev-list --objects --stdin --not --all --quiet --alternate-refs" code="0")
2023-02-22T07:24:40Z | 2023-02-22T07:24:40Z | 1 | main | ...version
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z | 1 | main | ...start (argv="git rev-list --objects --stdin --not --all --quiet --alternate-refs")
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z | 1 | main | ...def_repo
2023-02-22T07:24:39Z | 2023-02-22T07:24:39Z |   | main | ..fetch:consume_refs
2023-02-22T07:24:40Z | 2023-02-22T07:24:40Z |   | main | .submodule:parallel/fetch (msg="max:1")
2023-02-22T07:24:40Z | 2023-02-22T07:24:40Z | 2 | main | .child_start (argv="git maintenance run --auto --no-quiet" code="0")
2023-02-22T07:24:40Z | 2023-02-22T07:24:40Z | 2 | main | ..version
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z | 2 | main | ..start (argv="git maintenance run --auto --no-quiet")
2023-02-22T07:24:36Z | 2023-02-22T07:24:36Z | 2 | main | ..def_repo
`,
		},
		{
			desc:   "sampled git commit events",
			events: string(testhelper.MustReadFile(t, "testdata/git-commit.event")),
			expectedTrace: `
2023-02-22T11:26:37Z | 2023-02-22T11:26:38Z |   | main | root (code="0")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .version
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .start (argv="git commit --amend")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .def_repo
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .index:do_read_index (msg=".git/index")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..cache_tree:read
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:read/version (data="2")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:read/cache_nr (data="1590")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .index:preload
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:preload/sum_lstat (data="1590")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .index:preload
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:preload/sum_lstat (data="0")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .index:refresh
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:refresh/sum_lstat (data="0")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:refresh/sum_scan (data="0")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .cache_tree:update
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .index:do_write_index (msg="/gitaly/.git/index.lock")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..cache_tree:write
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:write/version (data="2")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..data:index:write/cache_nr (data="1590")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .status:worktrees
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..diff:setup
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..diff:write back to queue
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .status:index
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..unpack_trees:unpack_trees
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..diff:setup
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..diff:write back to queue
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .status:untracked
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | ..dir:read_directory
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .data:status:count/changed (data="5")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .data:status:count/untracked (data="0")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .data:status:count/ignored (data="0")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .status:print
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z |   | main | .cache_tree:update
2023-02-22T11:26:37Z | 2023-02-22T11:26:38Z | 0 | main | .child_start (argv="nvim /gitaly/.git/COMMIT_EDITMSG" code="0")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z | 0 | main | ..version
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z | 0 | main | ..start (argv="git diff --no-color --no-ext-diff -U0 -- COMMIT_EDITMSG")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z | 0 | main | ..error (msg="this operation must be run in a work tree")
2023-02-22T11:26:38Z | 2023-02-22T11:26:38Z | 0 | main | ..version
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z | 0 | main | ..start (argv="git branch --no-color --show-current")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z | 0 | main | ..def_repo
2023-02-22T11:26:38Z | 2023-02-22T11:26:38Z | 1 | main | .child_start (argv="git maintenance run --auto --no-quiet" code="0")
2023-02-22T11:26:38Z | 2023-02-22T11:26:38Z | 1 | main | ..version
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z | 1 | main | ..start (argv="git maintenance run --auto --no-quiet")
2023-02-22T11:26:37Z | 2023-02-22T11:26:37Z | 1 | main | ..def_repo
2023-02-22T11:26:38Z | 2023-02-22T11:26:38Z |   | main | .diff:setup
2023-02-22T11:26:38Z | 2023-02-22T11:26:38Z |   | main | .diff:write back to queue
2023-02-22T11:26:38Z | 2023-02-22T11:26:38Z |   | main | .data_json:traverse_trees:statistics (data="{\"traverse_trees_count\":2,\"traverse_trees_max_depth\":2}")
`,
		},
		{
			desc:   "sampled git pack objects events",
			events: string(testhelper.MustReadFile(t, "testdata/git-pack-objects.event")),
			expectedTrace: `
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | root (code="0")
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .version
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .start (argv="git pack-objects toon --compression=0")
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .def_repo
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .pack-objects:enumerate-objects
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | ..progress:Enumerating objects
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .pack-objects:prepare-pack
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | ..progress:Counting objects
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .pack-objects:write-pack-file
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | ..progress:Writing objects
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | ..data:pack-objects:write_pack_file/wrote (data="1")
2023-02-22T12:05:04Z | 2023-02-22T12:05:04Z |   | main | .data:fsync:fsync/writeout-only (data="2")
`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			trace, err := Parse(testhelper.Context(t), strings.NewReader(tc.events))

			if tc.expectedErr != "" {
				// JSON doesn't export error creation. It's not feasible to compare
				// error directly. We can only compare by the stringified error
				require.Error(t, err)
				require.Equal(t, tc.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, strings.TrimSpace(tc.expectedTrace), trace.Inspect(true))
		})
	}
}
