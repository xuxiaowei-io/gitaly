package commit

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

const (
	// InvalidUTF8PathPlaceholder is a placeholder we return in the Path field since
	// returning non utf8 data will result in a marshalling error
	// Once we deprecate the Path field, we can remove this
	InvalidUTF8PathPlaceholder = "ENCODING ERROR gitaly#1547"
)

var (
	maxNumStatBatchSize = 10
)

func (s *server) ListLastCommitsForTree(in *gitalypb.ListLastCommitsForTreeRequest, stream gitalypb.CommitService_ListLastCommitsForTreeServer) error {
	if err := validateListLastCommitsForTreeRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := listLastCommitsForTree(in, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func listLastCommitsForTree(in *gitalypb.ListLastCommitsForTreeRequest, stream gitalypb.CommitService_ListLastCommitsForTreeServer) error {
	cmd, parser, err := newLSTreeParser(in, stream)
	if err != nil {
		return err
	}

	ctx := stream.Context()
	repo := in.GetRepository()
	c, err := catfile.New(ctx, repo)
	if err != nil {
		return err
	}

	batch := make([]*gitalypb.ListLastCommitsForTreeResponse_CommitForTree, 0, maxNumStatBatchSize)
	entries, err := getLSTreeEntries(parser)
	if err != nil {
		return err
	}

	offset := int(in.GetOffset())
	if offset >= len(entries) {
		offset = 0
		entries = lstree.Entries{}
	}

	limit := offset + int(in.GetLimit())
	if limit > len(entries) {
		limit = len(entries)
	}

	for _, entry := range entries[offset:limit] {
		commit, err := log.LastCommitForPath(ctx, c, repo, string(in.GetRevision()), entry.Path)
		if err != nil {
			return err
		}

		commitForTree := &gitalypb.ListLastCommitsForTreeResponse_CommitForTree{
			PathBytes: []byte(entry.Path),
			Commit:    commit,
		}

		batch = append(batch, commitForTree)
		if len(batch) == maxNumStatBatchSize {
			if err := sendCommitsForTree(batch, stream); err != nil {
				return err
			}

			batch = batch[0:0]
		}
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return sendCommitsForTree(batch, stream)
}

func getLSTreeEntries(parser *lstree.Parser) (lstree.Entries, error) {
	entries := lstree.Entries{}

	for {
		entry, err := parser.NextEntry()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		entries = append(entries, *entry)
	}

	sort.Stable(entries)

	return entries, nil
}

func newLSTreeParser(in *gitalypb.ListLastCommitsForTreeRequest, stream gitalypb.CommitService_ListLastCommitsForTreeServer) (*command.Command, *lstree.Parser, error) {
	path := string(in.GetPath())
	if path == "" || path == "/" {
		path = "."
	}
	paths := strings.Split(path, " ")

	cmdArgs := append([]string{"ls-tree", "-z", "--full-name", string(in.GetRevision())}, paths...)
	cmd, err := git.Command(stream.Context(), in.GetRepository(), cmdArgs...)
	if err != nil {
		return nil, nil, err
	}

	return cmd, lstree.NewParser(cmd), nil
}

func sendCommitsForTree(batch []*gitalypb.ListLastCommitsForTreeResponse_CommitForTree, stream gitalypb.CommitService_ListLastCommitsForTreeServer) error {
	if len(batch) == 0 {
		return nil
	}

	if err := stream.Send(&gitalypb.ListLastCommitsForTreeResponse{Commits: batch}); err != nil {
		return err
	}

	return nil
}

func validateListLastCommitsForTreeRequest(in *gitalypb.ListLastCommitsForTreeRequest) error {
	if in.Revision == "" {
		return fmt.Errorf("empty Revision")
	}
	return nil
}
