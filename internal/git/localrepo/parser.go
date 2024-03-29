package localrepo

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// ErrParse is returned when the parse of an entry was unsuccessful
var ErrParse = errors.New("failed to parse git ls-tree response")

// Parser holds the necessary state for parsing the ls-tree output
type Parser struct {
	reader     *bufio.Reader
	objectHash git.ObjectHash
}

// NewParser returns a new Parser
func NewParser(src io.Reader, objectHash git.ObjectHash) *Parser {
	return &Parser{
		reader:     bufio.NewReader(src),
		objectHash: objectHash,
	}
}

// NextEntry reads a tree entry as it would be written by `git ls-tree -z`.
func (p *Parser) NextEntry() (*TreeEntry, error) {
	// Each tree entry is expected to have a format of `<mode> SP <type> SP <objectid> TAB <path> NUL`.
	treeEntryMode, err := p.reader.ReadBytes(' ')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("reading mode: %w", err)
	}
	treeEntryMode = treeEntryMode[:len(treeEntryMode)-1]

	treeEntryType, err := p.reader.ReadBytes(' ')
	if err != nil {
		return nil, fmt.Errorf("reading type: %w", err)
	}
	treeEntryType = treeEntryType[:len(treeEntryType)-1]

	treeEntryID, err := p.reader.ReadBytes('\t')
	if err != nil {
		return nil, fmt.Errorf("reading OID: %w", err)
	}
	treeEntryID = treeEntryID[:len(treeEntryID)-1]

	treeEntryPath, err := p.reader.ReadBytes(0x00)
	if err != nil {
		return nil, fmt.Errorf("reading path: %w", err)
	}
	treeEntryPath = treeEntryPath[:len(treeEntryPath)-1]

	objectID, err := p.objectHash.FromHex(string(treeEntryID))
	if err != nil {
		return nil, err
	}

	return &TreeEntry{
		Mode: string(treeEntryMode),
		OID:  objectID,
		Path: string(treeEntryPath),
		Type: ObjectTypeFromString(string(treeEntryType)),
	}, nil
}

// NextEntryPath reads the path of next entry as it would be written by `git ls-tree --name-only -z`.
func (p *Parser) NextEntryPath() ([]byte, error) {
	treeEntryPath, err := p.reader.ReadBytes(0x00)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("reading path: %w", err)
	}
	return treeEntryPath[:len(treeEntryPath)-1], nil
}
