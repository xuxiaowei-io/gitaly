package commit

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trailerparser"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

var statsPattern = regexp.MustCompile(`\s(\d+)\sfiles? changed(,\s(\d+)\sinsertions?\(\+\))?(,\s(\d+)\sdeletions?\(-\))?`)

func validateFindCommitsRequest(locator storage.Locator, in *gitalypb.FindCommitsRequest) error {
	if err := locator.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision(in.GetRevision(), git.AllowEmptyRevision()); err != nil {
		return err
	}
	return nil
}

func (s *server) FindCommits(req *gitalypb.FindCommitsRequest, stream gitalypb.CommitService_FindCommitsServer) error {
	ctx := stream.Context()

	if err := validateFindCommitsRequest(s.locator, req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(req.GetRepository())

	// Use Gitaly's default branch lookup function because that is already
	// migrated.
	if revision := req.Revision; len(revision) == 0 && !req.GetAll() {
		defaultBranch, err := repo.GetDefaultBranch(ctx)
		if err != nil {
			return structerr.NewInternal("defaultBranchName: %w", err)
		}
		req.Revision = []byte(defaultBranch)
	}
	// Clients might send empty paths. That is an error
	for _, path := range req.Paths {
		if len(path) == 0 {
			return structerr.NewInvalidArgument("path is empty string")
		}
	}

	if err := s.findCommits(ctx, req, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) findCommits(ctx context.Context, req *gitalypb.FindCommitsRequest, stream gitalypb.CommitService_FindCommitsServer) (returnedErr error) {
	commitCounter := int32(0)
	opts := git.ConvertGlobalOptions(req.GetGlobalOptions())
	repo := s.localrepo(req.GetRepository())

	var stderr strings.Builder
	gitLogCmd, limit := getLogCommandSubCmd(req)
	logCmd, err := repo.Exec(ctx, gitLogCmd, append(opts, git.WithSetupStdout(), git.WithStderr(&stderr))...)
	if err != nil {
		return fmt.Errorf("error when creating git log command: %w", err)
	}
	defer func() {
		if err := logCmd.Wait(); err != nil {
			// We differentiate benign errors from real errors in this deferred function.
			// Benign errors are the errors that are not harmful to the system, see the cases below.
			if limit >= 0 && commitCounter == limit {
				// Benign error caused by terminating the stream since response limit is reached;
				// We already send the maximum number of commits, so git log command is terminated.
				s.logger.Debug("git log command terminated because maximum number of commits is reached")
				return
			}

			// Real errors are caused by git log command failures such as timeout or OOM kill.
			if returnedErr == nil {
				// When returnedErr is nil, it means that we haven't encountered any errors in the findCommits function.
				// But git log command still failed, so we put the git log command error in returnedErr
				// and avoid overriding the real error.
				returnedErr = wrapGitLogCmdError(req.GetRevision(), commitCounter, err, stderr.String())
			} else {
				s.logger.WithError(err).WithField("stderr", stderr.String()).Error("listing commits failed")
			}
		}
	}()

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return fmt.Errorf("creating catfile: %w", err)
	}
	defer cancel()

	getCommits := NewGetCommits(logCmd, objectReader, req.GetIncludeShortstat())

	if calculateOffsetManually(req) {
		if err := getCommits.Offset(int(req.GetOffset())); err != nil {
			// If we're at EOF, then it means that the offset has been greater than the
			// number of available commits. We do not treat this as an error, but
			// instead just return EOF ourselves.
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("skipping to offset %d: %w", req.GetOffset(), err)
		}
	}

	commitCounter, err = streamCommits(getCommits, stream, req.GetTrailers(), req.GetIncludeShortstat(), len(req.GetIncludeReferencedBy()) > 0)
	if err != nil {
		return fmt.Errorf("error streaming commits: %w", err)
	}

	return nil
}

func calculateOffsetManually(req *gitalypb.FindCommitsRequest) bool {
	return req.GetFollow() && req.GetOffset() > 0
}

// GetCommits wraps a git log command that can be iterated on to get individual commit objects
type GetCommits struct {
	scanner      *bufio.Scanner
	objectReader catfile.ObjectContentReader
}

// NewGetCommits returns a new GetCommits object
func NewGetCommits(cmd *command.Command, objectReader catfile.ObjectContentReader, shortStat bool) *GetCommits {
	getCommits := &GetCommits{
		scanner:      bufio.NewScanner(cmd),
		objectReader: objectReader,
	}
	// If include shortstat, the scanner splits commits by special token,
	// so that the commit hash and stats are combined.
	if shortStat {
		getCommits.scanner.Split(splitStat)
	}
	return getCommits
}

// Scan indicates whether or not there are more commits to return
func (g *GetCommits) Scan() bool {
	return g.scanner.Scan()
}

// Err returns the first non EOF error
func (g *GetCommits) Err() error {
	return g.scanner.Err()
}

// Offset skips over a number of commits
func (g *GetCommits) Offset(offset int) error {
	for i := 0; i < offset; i++ {
		if !g.Scan() {
			err := g.Err()
			if err == nil {
				err = io.EOF
			}

			return fmt.Errorf("skipping commit: %w", err)
		}
	}
	return nil
}

// Commit returns the current commit
func (g *GetCommits) Commit(ctx context.Context, trailers, shortStat, refs bool) (*gitalypb.GitCommit, error) {
	logOutput := strings.TrimSpace(g.scanner.Text())
	var revAndTrailers []string
	var revAndStats []string
	var revAndRefs []string
	var revision string

	if shortStat {
		revAndStats = strings.SplitN(logOutput, "\n", 2)
		logOutput = revAndStats[0]
	}

	if refs {
		revAndRefs = strings.SplitN(logOutput, "\002", 2)
		logOutput = revAndRefs[0]
	}

	if trailers {
		revAndTrailers = strings.SplitN(logOutput, "\000", 2)
		revision = revAndTrailers[0]
	} else {
		revision = logOutput
	}

	commit, err := catfile.GetCommit(ctx, g.objectReader, git.Revision(revision))
	if err != nil {
		return nil, fmt.Errorf("cat-file get commit %q: %w", revision, err)
	}

	if refs && len(revAndRefs) == 2 {
		commit.ReferencedBy = parseRefs(revAndRefs[1])
	}

	if trailers && len(revAndTrailers) == 2 {
		commit.Trailers = trailerparser.Parse([]byte(revAndTrailers[1]))
	}

	if shortStat && len(revAndStats) == 2 {
		commit.ShortStats, err = parseStat(revAndStats[1])
		if err != nil {
			return nil, fmt.Errorf("get stats: %w", err)
		}
	}

	return commit, nil
}

func streamCommits(getCommits *GetCommits, stream gitalypb.CommitService_FindCommitsServer, trailers, shortStat bool, refs bool) (int32, error) {
	ctx := stream.Context()
	commitCounter := int32(0)

	chunker := chunk.New(&commitsSender{
		send: func(commits []*gitalypb.GitCommit) error {
			return stream.Send(&gitalypb.FindCommitsResponse{
				Commits: commits,
			})
		},
	})

	for getCommits.Scan() {
		commit, err := getCommits.Commit(ctx, trailers, shortStat, refs)
		if err != nil {
			return commitCounter, err
		}

		if err := chunker.Send(commit); err != nil {
			return commitCounter, err
		}
		commitCounter++
	}

	if getCommits.Err() != nil {
		return commitCounter, fmt.Errorf("get commits: %w", getCommits.Err())
	}

	return commitCounter, chunker.Flush()
}

func getLogCommandSubCmd(req *gitalypb.FindCommitsRequest) (git.Command, int32) {
	logFormatOption := "--format=%H"
	// To split the commits by '\x01' instead of '\n'
	if req.GetIncludeShortstat() {
		logFormatOption = "--format=%x01%H"
	}
	if req.GetTrailers() {
		logFormatOption += "%x00%(trailers:unfold,separator=%x00)"
	}

	if len(req.GetIncludeReferencedBy()) > 0 {
		// Delimit ref names with '\x02' to avoid confusing with trailers
		logFormatOption += "%x02%D"
	}

	subCmd := git.Command{Name: "log", Flags: []git.Option{git.Flag{Name: logFormatOption}}}

	//  We will perform the offset in Go because --follow doesn't play well with --skip.
	//  See: https://gitlab.com/gitlab-org/gitlab-ce/issues/3574#note_3040520
	if req.GetOffset() > 0 && !calculateOffsetManually(req) {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: fmt.Sprintf("--skip=%d", req.GetOffset())})
	}
	limit := req.GetLimit()
	if calculateOffsetManually(req) {
		limit += req.GetOffset()
	}
	subCmd.Flags = append(subCmd.Flags, git.Flag{Name: fmt.Sprintf("--max-count=%d", limit)})

	if req.GetFollow() && len(req.GetPaths()) > 0 {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--follow"})
	}
	if req.GetAuthor() != nil {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: fmt.Sprintf("--author=%s", string(req.GetAuthor()))})
	}
	if req.GetSkipMerges() {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--no-merges"})
	}
	if req.GetBefore() != nil {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: fmt.Sprintf("--before=%s", req.GetBefore().String())})
	}
	if req.GetAfter() != nil {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: fmt.Sprintf("--after=%s", req.GetAfter().String())})
	}
	if req.GetAll() {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--all"}, git.Flag{Name: "--reverse"})
	}
	if req.GetRevision() != nil {
		subCmd.Args = []string{string(req.GetRevision())}
	}
	if req.GetFirstParent() {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--first-parent"})
	}
	if len(req.GetPaths()) > 0 {
		for _, path := range req.GetPaths() {
			subCmd.PostSepArgs = append(subCmd.PostSepArgs, string(path))
		}
	}
	if req.GetOrder() == gitalypb.FindCommitsRequest_TOPO {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--topo-order"})
	}
	if req.GetIncludeShortstat() {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--shortstat"})
	}

	if len(req.GetIncludeReferencedBy()) > 0 {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--decorate=full"})
		for _, pattern := range req.GetIncludeReferencedBy() {
			subCmd.Flags = append(subCmd.Flags, git.Flag{Name: fmt.Sprintf("--decorate-refs=%s", pattern)})
		}
	}

	return subCmd, limit
}

func parseRefs(refsLine string) [][]byte {
	var refs [][]byte
	for _, ref := range strings.Split(refsLine, ", ") {
		if ref == "" {
			continue
		}

		// Tags are output as `tag: refs/tags/<name>`. Trim the tag prefix in case
		// this is a tag.
		ref = strings.TrimPrefix(ref, "tag: ")

		// By itself, HEAD is printed as HEAD. If HEAD points to another branch
		// that is output, for example refs/heads/master, then HEAD printed as
		// `HEAD -> refs/heads/master`. We need to separate the refs and include
		// both in the response if this is the case.
		if leftRef, rightRef, ok := strings.Cut(ref, " -> "); ok {
			refs = append(refs, []byte(leftRef), []byte(rightRef))
			continue
		}

		refs = append(refs, []byte(ref))
	}
	return refs
}

func parseStat(line string) (*gitalypb.CommitStatInfo, error) {
	statInfo := &gitalypb.CommitStatInfo{}

	matched := statsPattern.FindStringSubmatch(line)
	if len(matched) != 6 {
		return nil, fmt.Errorf("unexpected stats format: %q", line)
	}

	fileStr, addStr, delStr := matched[1], matched[3], matched[5]

	file64, err := strconv.ParseInt(fileStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parsing file count: %w", err)
	}
	statInfo.ChangedFiles = int32(file64)

	if len(addStr) > 0 {
		add64, err := strconv.ParseInt(addStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parsing additions: %w", err)
		}

		statInfo.Additions = int32(add64)
	}

	if len(delStr) > 0 {
		del64, err := strconv.ParseInt(delStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parsing deletions: %w", err)
		}

		statInfo.Deletions = int32(del64)
	}

	return statInfo, nil
}

func splitStat(data []byte, atEOF bool) (int, []byte, error) {
	// If there is no more data to be read then we are fine.
	if atEOF && len(data) == 0 {
		return 0, nil, io.EOF
	}

	// Commits are separated by `\x01` bytes, so we require each commit to start with it. If
	// that is not the case we return an error.
	if !bytes.HasPrefix(data, []byte{'\x01'}) {
		return 0, nil, fmt.Errorf("expected \\x01 prefix: %q", string(data))
	}

	// Skip the prefix. We only want to return the actual commit's data to the caller.
	data = data[1:]

	// We scan until the next `\x01` byte. If there is none, we're either at EOF (in which case
	// we just return remaining bytes) or we don't have sufficient data.
	index := bytes.IndexByte(data, '\x01')
	if index < 0 {
		if atEOF {
			return len(data) + 1, data, nil
		}

		return 0, nil, nil
	}

	return index + 1, data[:index], nil
}

// wrapGitError wraps git log error with a structError
func wrapGitLogCmdError(revision []byte, commitCounter int32, err error, stderr string) (structError structerr.Error) {
	// when `git log` returns empty and also exit with an error code.
	if commitCounter == 0 {
		switch {
		case strings.HasPrefix(stderr, "fatal: ambiguous argument "):
			// for example, git log non-existing-branch will cause
			// fatal: ambiguous argument 'non-existing-branch': unknown revision or path not in the working tree.
			return structerr.NewNotFound("commits not found").
				WithMetadata("error", err).
				WithMetadata("stderr", stderr).
				WithDetail(&gitalypb.FindCommitsError{
					Error: &gitalypb.FindCommitsError_AmbiguousRef{
						AmbiguousRef: &gitalypb.AmbiguousReferenceError{
							Reference: revision,
						},
					},
				})
		case strings.HasPrefix(stderr, "fatal: bad object"):
			// for example, git log 37811987837aacbd3b1d8ceb8de669b33f7c7c0a will cause
			// fatal: bad object 37811987837aacbd3b1d8ceb8de669b33f7c7c0a
			return structerr.NewNotFound("commits not found").
				WithMetadata("error", err).
				WithMetadata("stderr", stderr).
				WithDetail(&gitalypb.FindCommitsError{
					Error: &gitalypb.FindCommitsError_BadObject{
						BadObject: &gitalypb.BadObjectError{
							ObjectId: revision,
						},
					},
				})
		case strings.HasPrefix(stderr, "fatal: Invalid revision range"):
			// for example git log 37811987837aacbd3b1d8ceb8de669b33f7c7c0a..37811987837aacbd3b1d8ceb8de669b33f7c7c0b will cause
			// fatal: Invalid revision range 37811987837aacbd3b1d8ceb8de669b33f7c7c0a..37811987837aacbd3b1d8ceb8de669b33f7c7c0b
			return structerr.NewNotFound("commits not found").
				WithMetadata("error", err).
				WithMetadata("stderr", stderr).
				WithDetail(&gitalypb.FindCommitsError{
					Error: &gitalypb.FindCommitsError_InvalidRange{
						InvalidRange: &gitalypb.InvalidRevisionRange{
							Range: revision,
						},
					},
				})
		default:
			return structerr.NewNotFound("commits not found").
				// other cases we don't know right now, can be added when necessary
				WithMetadata("error", err).
				WithMetadata("stderr", stderr).
				WithDetail(&gitalypb.FindCommitsError{
					Error: &gitalypb.FindCommitsError_UncategorizedError{
						UncategorizedError: &gitalypb.UncategorizedGitLogError{},
					},
				})

		}
	}
	return structerr.NewInternal("listing commits failed").WithMetadata("error", err).WithMetadata("stderr", stderr)
}
