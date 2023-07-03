package repository

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// revisionNotFoundRegexp is used to parse the standard error of git-rev-list(1) in order to figure out whether the
// actual error condition was that the revision could not be found. Checking for ambiguous arguments is kind of awkward,
// but we can't really get around that until we have migrated to `--stdin` with Git v2.42 and later.
var revisionNotFoundRegexp = regexp.MustCompile("^fatal: ambiguous argument '([^']*)': unknown revision or path not in the working tree.")

// ObjectsSize calculates the on-disk object size via a graph walk. The intent of this RPC is to
// calculate the on-disk size as accurately as possible.
func (s *server) ObjectsSize(server gitalypb.RepositoryService_ObjectsSizeServer) (returnedErr error) {
	ctx := server.Context()

	request, err := server.Recv()
	if err != nil {
		return fmt.Errorf("receiving initial request: %w", err)
	}

	if err := s.locator.ValidateRepository(request.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(request.GetRepository())

	revlistArgs := make([]string, 0, len(request.GetRevisions()))

	// TODO: Git v2.41 and older do not support passing pseudo-options via `--stdin`. We have upstreamed patches to
	// fix this in Git v2.42, so once we have upgraded our Git version we should convert this loop to instead stream
	// to git-rev-list(1)'s standard input.
	for i := 0; ; i++ {
		if i != 0 && request.GetRepository() != nil {
			return structerr.NewInvalidArgument("subsequent requests must not contain repository")
		}

		if len(request.GetRevisions()) == 0 {
			return structerr.NewInvalidArgument("no revisions specified")
		}

		for _, revision := range request.GetRevisions() {
			if err := git.ValidateRevision(revision, git.AllowPseudoRevision()); err != nil {
				return structerr.NewInvalidArgument("validating revision: %w", err).WithMetadata("revision", revision)
			}

			revlistArgs = append(revlistArgs, string(revision))
		}

		request, err = server.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("receiving next request: %w", err)
		}
	}

	var stderr, stdout strings.Builder
	if err := repo.ExecAndWait(ctx, git.Command{
		Name: "rev-list",
		Flags: []git.Option{
			git.Flag{Name: "--disk-usage"},
			git.Flag{Name: "--objects"},
		},
		Args: revlistArgs,
	}, git.WithStderr(&stderr), git.WithStdout(&stdout)); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if matches := revisionNotFoundRegexp.FindStringSubmatch(stderr.String()); len(matches) == 2 {
				return structerr.NewNotFound("revision not found").WithMetadata("revision", matches[1])
			}

			return structerr.New("rev-list failed with error code %d", exitErr.ExitCode()).WithMetadata("stderr", stderr.String())
		}

		return fmt.Errorf("running git-rev-list: %w", err)
	}

	sizeString := strings.TrimSpace(stdout.String())
	size, err := strconv.ParseUint(sizeString, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing object size: %w", err)
	}

	if err := server.SendAndClose(&gitalypb.ObjectsSizeResponse{
		Size: size,
	}); err != nil {
		return fmt.Errorf("sending objects size: %w", err)
	}

	return nil
}
