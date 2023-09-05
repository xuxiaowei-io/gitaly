package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// FormatTagError is used by FormatTag() below
type FormatTagError struct {
	expectedLines int
	actualLines   int
}

func (e FormatTagError) Error() string {
	return fmt.Sprintf("should have %d tag header lines, got %d", e.expectedLines, e.actualLines)
}

// FormatTag is used by WriteTag (or for testing) to make the tag
// signature to feed to git-mktag, i.e. the plain-text mktag
// format. This does not create an object, just crafts input for "git
// mktag" to consume.
//
// We are not being paranoid about exhaustive input validation here
// because we're just about to run git's own "fsck" check on this.
//
// However, if someone injected parameters with extra newlines they
// could cause subsequent values to be ignored via a crafted
// message. This someone could also locally craft a tag locally and
// "git push" it. But allowing e.g. someone to provide their own
// timestamp here would at best be annoying, and at worst run up
// against some other assumption (e.g. that some hook check isn't as
// strict on locally generated data).
func FormatTag(
	objectID git.ObjectID,
	objectType string,
	tagName, tagBody []byte,
	committer *gitalypb.User,
	committerDate time.Time,
) (string, error) {
	if committerDate.IsZero() {
		committerDate = time.Now()
	}

	tagHeaderFormat := "object %s\n" +
		"type %s\n" +
		"tag %s\n" +
		"tagger %s <%s> %s\n"
	tagBuf := fmt.Sprintf(tagHeaderFormat, objectID.String(), objectType, tagName, committer.GetName(), committer.GetEmail(), git.FormatSignatureTime(committerDate))

	maxHeaderLines := 4
	actualHeaderLines := strings.Count(tagBuf, "\n")
	if actualHeaderLines != maxHeaderLines {
		return "", FormatTagError{expectedLines: maxHeaderLines, actualLines: actualHeaderLines}
	}

	tagBuf += "\n"
	tagBuf += string(tagBody)

	return tagBuf, nil
}

// MktagError is used by WriteTag() below
type MktagError struct {
	tagName []byte
	stderr  string
}

func (e MktagError) Error() string {
	// TODO: Upper-case error message purely for transitory backwards compatibility
	return fmt.Sprintf("Could not update refs/tags/%s. Please refresh and try again.", e.tagName)
}

// WriteTag writes a tag to the repository's object database with
// git-mktag and returns its object ID.
//
// It's important that this be git-mktag and not git-hash-object due
// to its fsck sanity checking semantics.
func (repo *Repo) WriteTag(
	ctx context.Context,
	objectID git.ObjectID,
	objectType string,
	tagName, tagBody []byte,
	committer *gitalypb.User,
	committerDate time.Time,
) (git.ObjectID, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	tagBuf, err := FormatTag(objectID, objectType, tagName, tagBody, committer, committerDate)
	if err != nil {
		return "", err
	}

	content := strings.NewReader(tagBuf)

	cmd, err := repo.Exec(ctx,
		git.Command{
			Name: "mktag",
		},
		git.WithStdin(content),
		git.WithStdout(stdout),
		git.WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", MktagError{tagName: tagName, stderr: stderr.String()}
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("detecting object hash: %w", err)
	}

	tagID, err := objectHash.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", fmt.Errorf("could not parse tag ID: %w", err)
	}

	return tagID, nil
}
