package conflicts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) ListConflictFiles(request *gitalypb.ListConflictFilesRequest, stream gitalypb.ConflictsService_ListConflictFilesServer) error {
	ctx := stream.Context()

	if err := validateListConflictFilesRequest(request); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(request.GetRepository())

	ours, err := repo.ResolveRevision(ctx, git.Revision(request.OurCommitOid+"^{commit}"))
	if err != nil {
		return structerr.NewFailedPrecondition("could not lookup 'our' OID: %w", err)
	}

	theirs, err := repo.ResolveRevision(ctx, git.Revision(request.TheirCommitOid+"^{commit}"))
	if err != nil {
		return structerr.NewFailedPrecondition("could not lookup 'their' OID: %w", err)
	}

	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return err
	}

	if featureflag.ListConflictFilesMergeTree.IsEnabled(ctx) {
		return s.conflictFilesWithGitMergeTree(ctx, request, stream, ours, theirs, repo)
	}

	return s.conflictFilesWithGit2Go(ctx, request, stream, ours, theirs, repo, repoPath)
}

func (s *server) conflictFilesWithGitMergeTree(
	ctx context.Context,
	request *gitalypb.ListConflictFilesRequest,
	stream gitalypb.ConflictsService_ListConflictFilesServer,
	ours, theirs git.ObjectID,
	repo *localrepo.Repo,
) error {
	var mergeConflictErr *localrepo.MergeTreeConflictError

	oid, err := repo.MergeTree(ctx, ours.String(), theirs.String(), localrepo.WithAllowUnrelatedHistories())
	if !errors.As(err, &mergeConflictErr) {
		return structerr.NewFailedPrecondition("expected merge conflict: %w", err)
	}

	type conflictHeader struct {
		theirPath    string
		ourPath      string
		ancestorPath string
		ourMode      int32
	}

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return err
	}
	defer cancel()

	// We need to combine data with same path, but we also want to retain
	// the ordering. We use a map to track the data with the same path, but
	// retain ordering by using a slice.
	pathToConflict := make(map[string]*conflictHeader)
	var conflicts []*conflictHeader

	for _, conflictFile := range mergeConflictErr.ConflictingFileInfo {
		val, ok := pathToConflict[conflictFile.FileName]
		if !ok {
			val = &conflictHeader{}
			conflicts = append(conflicts, val)
		}

		switch conflictFile.Stage {
		case localrepo.MergeStageAncestor:
			val.ancestorPath = conflictFile.FileName
		case localrepo.MergeStageOurs:
			val.ourPath = conflictFile.FileName
			val.ourMode = conflictFile.Mode
		case localrepo.MergeStageTheirs:
			val.theirPath = conflictFile.FileName
		}

		pathToConflict[conflictFile.FileName] = val
	}

	var conflictFiles []*gitalypb.ConflictFile
	msgSize := 0

	// Git2Go conflict markers have filenames and git-merge-tree(1) has commit OIDs,
	// to keep the content the same, we replace commit OID with filenames.
	replaceOids := func(conflict *conflictHeader, chunk []byte) []byte {
		chunk = bytes.ReplaceAll(chunk, []byte(request.OurCommitOid), []byte(conflict.ourPath))
		chunk = bytes.ReplaceAll(chunk, []byte(request.TheirCommitOid), []byte(conflict.theirPath))

		return chunk
	}

	for _, conflict := range conflicts {
		if !request.AllowTreeConflicts && (conflict.theirPath == "" || conflict.ourPath == "") {
			return structerr.NewFailedPrecondition("conflict side missing")
		}

		conflictFiles = append(conflictFiles, &gitalypb.ConflictFile{
			ConflictFilePayload: &gitalypb.ConflictFile_Header{
				Header: &gitalypb.ConflictFileHeader{
					CommitOid:    request.OurCommitOid,
					TheirPath:    []byte(conflict.theirPath),
					OurPath:      []byte(conflict.ourPath),
					AncestorPath: []byte(conflict.ancestorPath),
					OurMode:      conflict.ourMode,
				},
			},
		})

		path := conflict.ourPath
		if path == "" {
			path = conflict.theirPath
		}

		fileOID, err := repo.ResolveRevision(ctx, oid.Revision()+":"+git.Revision(path))
		if err != nil {
			return fmt.Errorf("getting file revision: %w", err)
		}

		object, err := objectReader.Object(ctx, fileOID.Revision())
		if err != nil {
			return fmt.Errorf("getting objectreader: %w", err)
		}

		var content bytes.Buffer
		_, err = content.ReadFrom(object)
		if err != nil && err != io.EOF {
			return structerr.NewInternal("reading conflict object: %w", err)
		}

		if !utf8.Valid(content.Bytes()) {
			return structerr.NewFailedPrecondition("unsupported encoding")
		}

		parsedContent := replaceOids(conflict, content.Bytes())
		contentLen := len(parsedContent)

		for i := 0; i < contentLen; i += streamio.WriteBufferSize {
			end := i + streamio.WriteBufferSize
			if contentLen < end {
				end = contentLen
			}

			conflictFiles = append(conflictFiles, &gitalypb.ConflictFile{
				ConflictFilePayload: &gitalypb.ConflictFile_Content{
					Content: parsedContent[i:end],
				},
			})

			// We don't send a message for each chunk because the content of
			// a file may be smaller than the size limit, which means we can
			// keep adding data to the message
			msgSize += end - i
			if msgSize < streamio.WriteBufferSize {
				continue
			}

			if err := stream.Send(&gitalypb.ListConflictFilesResponse{
				Files: conflictFiles,
			}); err != nil {
				return structerr.NewInternal("error streaming conflict files: %w", err)
			}

			conflictFiles = conflictFiles[:0]
			msgSize = 0
		}
	}

	// Send leftover data, if any
	if len(conflictFiles) > 0 {
		if err := stream.Send(&gitalypb.ListConflictFilesResponse{
			Files: conflictFiles,
		}); err != nil {
			return structerr.NewInternal("error streaming conflict files: %w", err)
		}
	}

	return nil
}

func (s *server) conflictFilesWithGit2Go(
	ctx context.Context,
	request *gitalypb.ListConflictFilesRequest,
	stream gitalypb.ConflictsService_ListConflictFilesServer,
	ours, theirs git.ObjectID,
	repo *localrepo.Repo,
	repoPath string,
) error {
	conflicts, err := s.git2goExecutor.Conflicts(ctx, repo, git2go.ConflictsCommand{
		Repository: repoPath,
		Ours:       ours.String(),
		Theirs:     theirs.String(),
	})
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return structerr.NewInvalidArgument("%w", err)
		}
		return structerr.NewInternal("%w", err)
	}

	var conflictFiles []*gitalypb.ConflictFile
	msgSize := 0

	for _, conflict := range conflicts.Conflicts {
		if !request.AllowTreeConflicts && (conflict.Their.Path == "" || conflict.Our.Path == "") {
			return structerr.NewFailedPrecondition("conflict side missing")
		}

		if !utf8.Valid(conflict.Content) {
			return structerr.NewFailedPrecondition("unsupported encoding")
		}

		conflictFiles = append(conflictFiles, &gitalypb.ConflictFile{
			ConflictFilePayload: &gitalypb.ConflictFile_Header{
				Header: &gitalypb.ConflictFileHeader{
					CommitOid:    request.OurCommitOid,
					TheirPath:    []byte(conflict.Their.Path),
					OurPath:      []byte(conflict.Our.Path),
					AncestorPath: []byte(conflict.Ancestor.Path),
					OurMode:      conflict.Our.Mode,
				},
			},
		})

		contentReader := bytes.NewReader(conflict.Content)
		for {
			chunk := make([]byte, streamio.WriteBufferSize-msgSize)
			bytesRead, err := contentReader.Read(chunk)
			if err != nil && err != io.EOF {
				return structerr.NewInternal("%w", err)
			}

			if bytesRead > 0 {
				conflictFiles = append(conflictFiles, &gitalypb.ConflictFile{
					ConflictFilePayload: &gitalypb.ConflictFile_Content{
						Content: chunk[:bytesRead],
					},
				})
			}

			if err == io.EOF {
				break
			}

			// We don't send a message for each chunk because the content of
			// a file may be smaller than the size limit, which means we can
			// keep adding data to the message
			msgSize += bytesRead
			if msgSize < streamio.WriteBufferSize {
				continue
			}

			if err := stream.Send(&gitalypb.ListConflictFilesResponse{
				Files: conflictFiles,
			}); err != nil {
				return structerr.NewInternal("%w", err)
			}

			conflictFiles = conflictFiles[:0]
			msgSize = 0
		}
	}

	// Send leftover data, if any
	if len(conflictFiles) > 0 {
		if err := stream.Send(&gitalypb.ListConflictFilesResponse{
			Files: conflictFiles,
		}); err != nil {
			return structerr.NewInternal("%w", err)
		}
	}

	return nil
}

func validateListConflictFilesRequest(in *gitalypb.ListConflictFilesRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if in.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
	}
	if in.GetTheirCommitOid() == "" {
		return fmt.Errorf("empty TheirCommitOid")
	}

	return nil
}
