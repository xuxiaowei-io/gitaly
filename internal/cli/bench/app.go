package bench

import (
	"encoding/json"
	"io"

	"github.com/urfave/cli/v2"
)

const (
	startGitalyAction = "start_gitaly"
	stopGitalyAction  = "stop_gitaly"
	exitCoordAction   = "exit"
)

// CoordCmd is a command sent from the client to the coordinator.
type CoordCmd struct {
	Action string
	OutDir string
	Repo   string
	RPC    string
}

// CoordResp is response from the coordinator to the client indicating
// if an error has occurred.
type CoordResp struct {
	Error string
}

type jsonRW struct {
	encoder *json.Encoder
	decoder *json.Decoder
}

func newJSONRW(rw io.ReadWriter) *jsonRW {
	return &jsonRW{
		encoder: json.NewEncoder(rw),
		decoder: json.NewDecoder(rw),
	}
}

// NewApp returns a new gitaly-bench app.
func NewApp() *cli.App {
	return &cli.App{
		Name:  "gitaly-bench",
		Usage: "coordinate starting and stopping Gitaly for benchmarking",
		Commands: []*cli.Command{
			{
				Name:  "coordinate",
				Usage: "coordinate starting and stopping Gitaly for benchmarking",
				Description: "Handle starting and stopping Gitaly for benchmarking, directed " +
					"by the client. This will listen on a separate TCP port from Gitaly.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "repo-dir",
						Usage:    "Directory containing git repositories",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "coord-port",
						Value: "7075",
						Usage: "TCP port for coordinator to listen on",
					},
				},
				Action: RunCoordinator,
			},
		},
	}
}
