package catfile

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// MaxTagReferenceDepth is the maximum depth of tag references we will dereference
	MaxTagReferenceDepth = 10
)

// GetTag looks up a commit by tagID using an existing catfile.Batch instance. When 'trim' is
// 'true', the tag message will be trimmed to fit in a gRPC message. When 'trimRightNewLine' is
// 'true', the tag message will be trimmed to remove all '\n' characters from right. note: we pass
// in the tagName because the tag name from refs/tags may be different than the name found in the
// actual tag object. We want to use the tagName found in refs/tags
func GetTag(ctx context.Context, objectReader ObjectReader, tagID git.Revision, tagName string, trimLen, trimRightNewLine bool) (*gitalypb.Tag, error) {
	object, err := objectReader.Object(ctx, tagID)
	if err != nil {
		return nil, err
	}

	if object.Type != "tag" {
		if _, err := io.Copy(io.Discard, object); err != nil {
			return nil, fmt.Errorf("discarding object: %w", err)
		}

		return nil, NotFoundError{
			error: fmt.Errorf("expected tag, got %s", object.Type),
		}
	}

	tag, err := buildAnnotatedTag(ctx, objectReader, object, []byte(tagName), trimLen, trimRightNewLine)
	if err != nil {
		return nil, err
	}

	return tag, nil
}

// ExtractTagSignature extracts the signature from a content and returns both the signature
// and the remaining content. If no signature is found, nil as the signature and the entire
// content are returned. note: tags contain the signature block at the end of the message
// https://github.com/git/git/blob/master/Documentation/technical/signature-format.txt#L12
func ExtractTagSignature(content []byte) ([]byte, []byte) {
	index := bytes.Index(content, []byte("-----BEGIN"))

	if index > 0 {
		return bytes.TrimSuffix(content[index:], []byte("\n")), content[:index]
	}
	return nil, content
}

type tagHeader struct {
	oid     string
	tagType string
	tag     string
	tagger  string
}

func splitRawTag(object git.Object, trimRightNewLine bool) (*tagHeader, []byte, error) {
	raw, err := io.ReadAll(object)
	if err != nil {
		return nil, nil, err
	}

	var body []byte
	split := bytes.SplitN(raw, []byte("\n\n"), 2)
	if len(split) == 2 {
		body = split[1]
		if trimRightNewLine {
			// Remove trailing newline, if any, to preserve existing behavior the old GitLab tag finding code.
			// See https://gitlab.com/gitlab-org/gitaly/blob/5e94dc966ac1900c11794b107a77496552591f9b/ruby/lib/gitlab/git/repository.rb#L211.
			// Maybe this belongs in the FindAllTags handler, or even on the gitlab-ce client side, instead of here?
			body = bytes.TrimRight(body, "\n")
		}
	}

	var header tagHeader
	s := bufio.NewScanner(bytes.NewReader(split[0]))
	for s.Scan() {
		headerSplit := strings.SplitN(s.Text(), " ", 2)
		if len(headerSplit) != 2 {
			continue
		}

		key, value := headerSplit[0], headerSplit[1]
		switch key {
		case "object":
			header.oid = value
		case "type":
			header.tagType = value
		case "tag":
			header.tag = value
		case "tagger":
			header.tagger = value
		}
	}

	return &header, body, nil
}

// ParseTag parses the given object, which is expected to refer to a Git tag. The tag's tagged
// commit is not populated. The given object ID shall refer to the tag itself such that the returned
// Tag structure has the correct OID.
func ParseTag(object git.Object) (*gitalypb.Tag, error) {
	tag, _, err := parseTag(object, nil, true, true)
	return tag, err
}

func parseTag(object git.Object, name []byte, trimLen, trimRightNewLine bool) (*gitalypb.Tag, *tagHeader, error) {
	header, body, err := splitRawTag(object, trimRightNewLine)
	if err != nil {
		return nil, nil, err
	}

	if len(name) == 0 {
		name = []byte(header.tag)
	}

	tag := &gitalypb.Tag{
		Id:          object.ObjectID().String(),
		Name:        name,
		MessageSize: int64(len(body)),
		Message:     body,
	}

	if max := helper.MaxCommitOrTagMessageSize; trimLen && len(body) > max {
		tag.Message = tag.Message[:max]
	}

	signature, _ := ExtractTagSignature(body)
	if signature != nil {
		length := bytes.Index(signature, []byte("\n"))

		if length > 0 {
			signature := string(signature[:length])
			tag.SignatureType = detectSignatureType(signature)
		}
	}

	tag.Tagger = parseCommitAuthor(header.tagger)

	return tag, header, nil
}

func buildAnnotatedTag(ctx context.Context, objectReader ObjectReader, object git.Object, name []byte, trimLen, trimRightNewLine bool) (*gitalypb.Tag, error) {
	tag, header, err := parseTag(object, name, trimLen, trimRightNewLine)
	if err != nil {
		return nil, err
	}

	switch header.tagType {
	case "commit":
		tag.TargetCommit, err = GetCommit(ctx, objectReader, git.Revision(header.oid))
		if err != nil {
			return nil, fmt.Errorf("buildAnnotatedTag error when getting target commit: %v", err)
		}

	case "tag":
		tag.TargetCommit, err = dereferenceTag(ctx, objectReader, git.Revision(header.oid))
		if err != nil {
			return nil, fmt.Errorf("buildAnnotatedTag error when dereferencing tag: %v", err)
		}
	}

	return tag, nil
}

// dereferenceTag recursively dereferences annotated tags until it finds a commit.
// This matches the original behavior in the ruby implementation.
// we also protect against circular tag references. Even though this is not possible in git,
// we still want to protect against an infinite looop
func dereferenceTag(ctx context.Context, objectReader ObjectReader, oid git.Revision) (*gitalypb.GitCommit, error) {
	for depth := 0; depth < MaxTagReferenceDepth; depth++ {
		object, err := objectReader.Object(ctx, oid)
		if err != nil {
			return nil, err
		}

		switch object.Type {
		case "tag":
			header, _, err := splitRawTag(object, true)
			if err != nil {
				return nil, err
			}

			oid = git.Revision(header.oid)
			continue
		case "commit":
			return ParseCommit(object, object.ObjectInfo.Oid)
		default: // This current tag points to a tree or a blob
			// We do not care whether discarding the object fails -- the worst that can
			// happen is that the object reader is dirty after the RPC call finishes,
			// and then we'll just create a new one when we require it again.
			_, _ = io.Copy(io.Discard, object)
			return nil, nil
		}
	}

	// at this point the tag nesting has gone too deep. We want to return silently here however, as we don't
	// want to fail the entire request if one tag is nested too deeply.
	return nil, nil
}
