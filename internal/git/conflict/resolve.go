package conflict

import (
	"errors"
)

// section denotes the various conflict sections
type section uint

const (
	// Resolutions for conflict is done by clients providing selections for
	// the different conflict sections. "head"/"origin" is used to denote the
	// selection for ours/their trees respectively.
	head   = "head"
	origin = "origin"

	// fileLimit is used to set the limit on the buffer size for bufio.Scanner,
	// we don't support conflict resolution for files which require bigger buffers.
	fileLimit = 200 * (1 << 10)
)

const (
	// The sections are used to define various lines of the conflicted file.
	// Here Old/New is used to denote ours/their respectively.
	sectionNone = section(iota)
	sectionOld
	sectionNew
	sectionNoNewline
)

// Errors that can occur during parsing of a merge conflict file
var (
	// ErrUnmergeableFile is returned when the either the file exceeds the
	// fileLimit or no data was read from the file (in case of a binary file).
	ErrUnmergeableFile = errors.New("merging is not supported for file")
	// ErrUnexpectedDelimiter is returned  when the previous section doesn't
	// match the expected flow of sections.
	ErrUnexpectedDelimiter = errors.New("unexpected conflict delimiter")
	// ErrMissingEndDelimiter is returned when the final section parsed doesn't
	// match the expected end state.
	ErrMissingEndDelimiter = errors.New("missing last delimiter")
)

// line is a structure used to denote individual lines in the conflicted blob,
// with information around how that line maps to ours/theirs blobs.
type line struct {
	// objIndex states the cursor position in the conflicted blob
	objIndex uint
	// oldIndex states the cursor position in the 'ours' blob
	oldIndex uint
	// oldIndex states the cursor position in the 'theirs' blob
	newIndex uint
	// payload denotes the content of line (sans the newline)
	payload string
	// section denotes which section this line belongs to.
	section section
}

// Resolution indicates how to resolve a conflict
type Resolution struct {
	// OldPath is the mapping of the path wrt to 'ours' OID
	OldPath string `json:"old_path"`
	// OldPath is the mapping of the path wrt to 'their' OID
	NewPath string `json:"new_path"`

	// Sections is a map which is used to denote which section to select
	// for each conflict. Key is the sectionID, while the value is either
	// "head" or "origin", which denotes the ours/theirs OIDs respectively.
	Sections map[string]string `json:"sections"`

	// Content is used when no sections are defined
	Content string `json:"content"`
}
