package git

import (
	"fmt"
	"path/filepath"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// NewTreeEntry is a helper to construct a gitalypb.TreeEntry from the provided parameters.
func NewTreeEntry(commitOid, rootPath string, filename, oidBytes, modeBytes []byte) (*gitalypb.TreeEntry, error) {
	var objectType gitalypb.TreeEntry_EntryType

	mode, err := strconv.ParseInt(string(modeBytes), 8, 32)
	if err != nil {
		return nil, fmt.Errorf("parse mode: %w", err)
	}

	oid := fmt.Sprintf("%02x", oidBytes)

	// Based on https://github.com/git/git/blob/v2.13.1/builtin/ls-tree.c#L67-L87
	switch mode & 0xf000 {
	case 0o160000:
		objectType = gitalypb.TreeEntry_COMMIT
	case 0o40000:
		objectType = gitalypb.TreeEntry_TREE
	default:
		objectType = gitalypb.TreeEntry_BLOB
	}

	return &gitalypb.TreeEntry{
		CommitOid: commitOid,
		Oid:       oid,
		Path:      []byte(filepath.Join(rootPath, string(filename))),
		Type:      objectType,
		Mode:      int32(mode),
	}, nil
}
