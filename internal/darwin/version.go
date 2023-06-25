package darwin

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
)

var (
	majorVersion     int
	majorVersionOnce sync.Once
)

// MajorVersion returns a major number of the MacOS version.
func MajorVersion() int {
	majorVersionOnce.Do(func() {
		var buffer strings.Builder
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cmd, err := command.New(ctx, []string{"sw_vers", "-productVersion"}, command.WithStdout(&buffer))
		if err != nil {
			panic(fmt.Errorf("executing sw_vers command: %w", err))
		}
		if err := cmd.Wait(); err != nil {
			panic(fmt.Errorf("waiting for sw_vers command completion: %w", err))
		}
		parts := strings.Split(buffer.String(), ".")
		if len(parts) > 0 {
			version, err := strconv.ParseFloat(parts[0], 32)
			if err != nil {
				panic(fmt.Errorf("parse mac version: %w", err))
			}
			majorVersion = int(version)
		}
	})

	return majorVersion
}
