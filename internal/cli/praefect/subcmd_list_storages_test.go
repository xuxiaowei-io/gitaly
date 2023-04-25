package praefect

import (
	"bytes"
	"testing"

	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/config"
)

func TestListStoragesSubcommand(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc            string
		virtualStorages []*config.VirtualStorage
		args            []string
		expectedOutput  func(*tablewriter.Table)
	}{
		{
			desc: "one virtual storage",
			virtualStorages: []*config.VirtualStorage{
				{
					Name: "vs-1",
					Nodes: []*config.Node{
						{
							Storage: "storage-1",
							Address: "tcp://1.2.3.4",
						},
						{
							Storage: "storage-2",
							Address: "tcp://4.3.2.1",
						},
					},
				},
			},
			args: []string{},
			expectedOutput: func(t *tablewriter.Table) {
				t.Append([]string{"vs-1", "storage-1", "tcp://1.2.3.4"})
				t.Append([]string{"vs-1", "storage-2", "tcp://4.3.2.1"})
			},
		},
		{
			desc: "multiple virtual storages but only show one",
			virtualStorages: []*config.VirtualStorage{
				{
					Name: "vs-1",
					Nodes: []*config.Node{
						{
							Storage: "storage-1",
							Address: "tcp://1.2.3.4",
						},
						{
							Storage: "storage-2",
							Address: "tcp://4.3.2.1",
						},
					},
				},
				{
					Name: "vs-2",
					Nodes: []*config.Node{
						{
							Storage: "storage-3",
							Address: "tcp://1.1.3.4",
						},
						{
							Storage: "storage-4",
							Address: "tcp://1.3.2.1",
						},
					},
				},
				{
					Name: "vs-3",
					Nodes: []*config.Node{
						{
							Storage: "storage-5",
							Address: "tcp://2.1.3.4",
						},
						{
							Storage: "storage-6",
							Address: "tcp://2.3.2.1",
						},
					},
				},
			},
			args: []string{"-virtual-storage", "vs-2"},
			expectedOutput: func(t *tablewriter.Table) {
				t.Append([]string{"vs-2", "storage-3", "tcp://1.1.3.4"})
				t.Append([]string{"vs-2", "storage-4", "tcp://1.3.2.1"})
			},
		},
		{
			desc: "one virtual storage with virtual storage arg",
			virtualStorages: []*config.VirtualStorage{
				{
					Name: "vs-1",
					Nodes: []*config.Node{
						{
							Storage: "storage-1",
							Address: "tcp://1.2.3.4",
						},
						{
							Storage: "storage-2",
							Address: "tcp://4.3.2.1",
						},
					},
				},
			},
			args: []string{"-virtual-storage", "vs-1"},
			expectedOutput: func(t *tablewriter.Table) {
				t.Append([]string{"vs-1", "storage-1", "tcp://1.2.3.4"})
				t.Append([]string{"vs-1", "storage-2", "tcp://4.3.2.1"})
			},
		},
	}

	exec := func(confPath string, args []string) (string, error) {
		var stdout bytes.Buffer
		app := cli.App{
			Writer: &stdout,
			Commands: []*cli.Command{
				newListStoragesCommand(),
			},
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "config",
					Value: confPath,
				},
			},
		}
		err := app.Run(append([]string{progname, "list-storages"}, args...))
		return stdout.String(), err
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var expectedOutput bytes.Buffer
			table := tablewriter.NewWriter(&expectedOutput)
			table.SetHeader([]string{"VIRTUAL_STORAGE", "NODE", "ADDRESS"})
			table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
			table.SetAutoFormatHeaders(false)
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.SetCenterSeparator("")
			table.SetColumnSeparator("")
			table.SetRowSeparator("")
			table.SetHeaderLine(false)
			table.SetBorder(false)
			table.SetTablePadding("\t") // pad with tabs
			table.SetNoWhiteSpace(true)
			tc.expectedOutput(table)
			table.Render()

			conf := config.Config{
				ListenAddr:      ":0",
				VirtualStorages: tc.virtualStorages,
			}
			confPath := writeConfigToFile(t, conf)
			stdout, err := exec(confPath, tc.args)
			require.NoError(t, err)
			require.Equal(t, expectedOutput.String(), stdout)
		})
	}

	t.Run("negative", func(t *testing.T) {
		conf := config.Config{
			ListenAddr: ":0",
			VirtualStorages: []*config.VirtualStorage{
				{
					Name: "vs-1",
					Nodes: []*config.Node{
						{
							Storage: "storage-1",
							Address: "tcp://1.2.3.4",
						},
						{
							Storage: "storage-2",
							Address: "tcp://4.3.2.1",
						},
					},
				},
			},
		}
		confPath := writeConfigToFile(t, conf)

		t.Run("virtual storage arg matches no virtual storages", func(t *testing.T) {
			stdout, err := exec(confPath, []string{"-virtual-storage", "vs-2"})
			require.NoError(t, err)
			require.Equal(t, "No virtual storages named vs-2.\n", stdout)
		})

		t.Run("positional arguments", func(t *testing.T) {
			_, err := exec(confPath, []string{"-virtual-storage", "vs-1", "positional-arg"})
			require.Equal(t, cli.Exit(unexpectedPositionalArgsError{Command: "list-storages"}, 1), err)
		})
	})
}
