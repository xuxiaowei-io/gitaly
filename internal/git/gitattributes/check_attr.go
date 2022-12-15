package gitattributes

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

// endOfAttributes is a magic attribute name we use to detect all attributes
// have been processed. It is added to the end of the list of attributes the
// caller requests, when this attribute is seen we know it's the last one and we
// can break the loop.
const endOfAttributes = "end-of-attributes-----"

// CheckAttrCmd can be used to get the gitattributes(5) for a set of files in a
// repo.
type CheckAttrCmd struct {
	cmd    *command.Command
	stdout *bufio.Reader
	stdin  *bufio.Writer

	m *sync.Mutex
}

// CheckAttr creates a CheckAttrCmd that checks the given list of attribute names.
func CheckAttr(ctx context.Context, repo git.RepositoryExecutor, names []string) (*CheckAttrCmd, func(), error) {
	cmd, err := repo.Exec(ctx, git.Command{
		Name: "check-attr",
		Flags: []git.Option{
			git.Flag{Name: "--stdin"},
			git.Flag{Name: "-z"},
		},
		Args: append(names, endOfAttributes),
	},
		git.WithSetupStdin(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("start check-attr command: %w", err)
	}

	checker := CheckAttrCmd{
		cmd:    cmd,
		stdout: bufio.NewReader(cmd),
		stdin:  bufio.NewWriter(cmd),
		m:      &sync.Mutex{},
	}

	return &checker, func() { _ = cmd.Wait() }, nil
}

// Check the attributes for the file at the given path.
func (c CheckAttrCmd) Check(path string) (Attributes, error) {
	if strings.Contains(path, "\000") {
		return nil, fmt.Errorf("path with NUL byte not allowed")
	}

	c.m.Lock()
	defer c.m.Unlock()

	if _, err := c.stdin.WriteString(path + "\000"); err != nil {
		return nil, fmt.Errorf("write path: %w", err)
	}
	if err := c.stdin.Flush(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}

	attrs := Attributes{}
	buf := make([]string, 0, 3)

	// Using git-check-attr(1) with -z will return data in the format:
	// <path> NUL <attribute> NUL <info> NUL ...
	for {
		word, err := c.stdout.ReadBytes('\000')
		if err != nil {
			return nil, fmt.Errorf("read line: %w", err)
		}

		buf = append(buf, string(bytes.TrimSuffix(word, []byte{0})))

		if len(buf) < 3 {
			continue // Keep going until we have 3 words
		}

		if buf[0] != path {
			return nil, fmt.Errorf("wrong path name detected, expected %q, got %q", path, buf[0])
		}
		if buf[1] == endOfAttributes {
			break
		}
		if buf[2] != Unspecified {
			attrs = append(attrs, Attribute{Name: buf[1], State: buf[2]})
		}

		buf = buf[:0]
	}

	return attrs, nil
}
