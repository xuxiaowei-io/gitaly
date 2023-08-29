package git

import (
	"strings"
	"time"
)

const (
	// rfc2822DateFormat is the date format that Git typically uses for dates.
	rfc2822DateFormat = "Mon Jan 02 2006 15:04:05 -0700"
)

var signatureSanitizer = strings.NewReplacer("\n", "", "<", "", ">", "")

// Signature represents a commits signature.
type Signature struct {
	// Name of the author or the committer.
	Name string
	// Email of the author or the committer.
	Email string
	// When is the time of the commit.
	When time.Time
}

// NewSignature creates a new sanitized signature.
func NewSignature(name, email string, when time.Time) Signature {
	return Signature{
		Name:  signatureSanitizer.Replace(name),
		Email: signatureSanitizer.Replace(email),
		When:  when.Truncate(time.Second),
	}
}

// FormatTime formats the given time such that it can be used by Git.
//
// The formatted string uses RFC2822, which is typically used in the context of emails to format dates and which is well
// understood by Git in many contexts. This is _not_ usable though when you want to write a signature date directly into
// a Git object. In all other contexts, e.g. when passing a date via `GIT_COMMITTER_DATE`, it is preferable to use this
// format as it is unambiguous to Git. Unix timestamps are only recognized once they have at least 8 digits, which would
// thus rule all commit dates before 1970-04-26 17:46:40 +0000 UTC. While this is only ~4 months that we'd be missing
// since the birth of Unix timestamps, especially the zero date is likely going to come up frequently.
func FormatTime(t time.Time) string {
	return t.Format(rfc2822DateFormat)
}
