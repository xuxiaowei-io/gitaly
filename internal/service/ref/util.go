package ref

import (
	"bytes"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var localBranchFormatFields = []string{"%(refname)", "%(objectname)"}

func parseRef(ref []byte) ([][]byte, error) {
	elements := bytes.Split(ref, []byte("\x00"))
	if len(elements) != len(localBranchFormatFields) {
		return nil, status.Errorf(codes.Internal, "error parsing ref %q", ref)
	}
	return elements, nil
}

func buildLocalBranch(name []byte, target *gitalypb.GitCommit) *gitalypb.FindLocalBranchResponse {
	response := &gitalypb.FindLocalBranchResponse{
		Name: name,
	}

	if target == nil {
		return response
	}

	response.CommitId = target.Id
	response.CommitSubject = target.Subject

	if author := target.Author; author != nil {
		response.CommitAuthor = &gitalypb.FindLocalBranchCommitAuthor{
			Name:  author.Name,
			Email: author.Email,
			Date:  author.Date,
		}
	}

	if committer := target.Committer; committer != nil {
		response.CommitCommitter = &gitalypb.FindLocalBranchCommitAuthor{
			Name:  committer.Name,
			Email: committer.Email,
			Date:  committer.Date,
		}
	}

	return response
}

func buildAllBranchesBranch(c *catfile.Batch, elements [][]byte) (*gitalypb.FindAllBranchesResponse_Branch, error) {
	target, err := log.GetCommitCatfile(c, string(elements[1]))
	if err != nil {
		return nil, err
	}

	return &gitalypb.FindAllBranchesResponse_Branch{
		Name:   elements[0],
		Target: target,
	}, nil
}

func buildBranch(c *catfile.Batch, elements [][]byte) (*gitalypb.Branch, error) {
	target, err := log.GetCommitCatfile(c, string(elements[1]))
	if err != nil {
		return nil, err
	}

	return &gitalypb.Branch{
		Name:         elements[0],
		TargetCommit: target,
	}, nil
}

func newFindAllBranchNamesWriter(stream gitalypb.Ref_FindAllBranchNamesServer) lines.Sender {
	return func(refs [][]byte) error {
		return stream.Send(&gitalypb.FindAllBranchNamesResponse{Names: refs})
	}
}

func newFindAllTagNamesWriter(stream gitalypb.Ref_FindAllTagNamesServer) lines.Sender {
	return func(refs [][]byte) error {
		return stream.Send(&gitalypb.FindAllTagNamesResponse{Names: refs})
	}
}

func newFindLocalBranchesWriter(stream gitalypb.Ref_FindLocalBranchesServer, c *catfile.Batch) lines.Sender {
	return func(refs [][]byte) error {
		var branches []*gitalypb.FindLocalBranchResponse

		for _, ref := range refs {
			elements, err := parseRef(ref)
			if err != nil {
				return err
			}

			target, err := log.GetCommitCatfile(c, string(elements[1]))
			if err != nil {
				return err
			}

			branches = append(branches, buildLocalBranch(elements[0], target))
		}
		return stream.Send(&gitalypb.FindLocalBranchesResponse{Branches: branches})
	}
}

func newFindAllBranchesWriter(stream gitalypb.RefService_FindAllBranchesServer, c *catfile.Batch) lines.Sender {
	return func(refs [][]byte) error {
		var branches []*gitalypb.FindAllBranchesResponse_Branch

		for _, ref := range refs {
			elements, err := parseRef(ref)
			if err != nil {
				return err
			}
			branch, err := buildAllBranchesBranch(c, elements)
			if err != nil {
				return err
			}
			branches = append(branches, branch)
		}
		return stream.Send(&gitalypb.FindAllBranchesResponse{Branches: branches})
	}
}

func newFindAllRemoteBranchesWriter(stream gitalypb.RefService_FindAllRemoteBranchesServer, c *catfile.Batch) lines.Sender {
	return func(refs [][]byte) error {
		var branches []*gitalypb.Branch

		for _, ref := range refs {
			elements, err := parseRef(ref)
			if err != nil {
				return err
			}
			branch, err := buildBranch(c, elements)
			if err != nil {
				return err
			}
			branches = append(branches, branch)
		}

		return stream.Send(&gitalypb.FindAllRemoteBranchesResponse{Branches: branches})
	}
}
