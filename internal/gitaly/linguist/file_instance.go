package linguist

import (
	"fmt"
	"io"

	"github.com/go-enry/go-enry/v2"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gitpipe"
)

type fileInstance struct {
	filename string
}

func newFileInstance(filename string) fileInstance {
	return fileInstance{
		filename: filename,
	}
}

func (f fileInstance) isDocumentation() bool {
	return enry.IsDocumentation(f.filename)
}

func (f fileInstance) isVendored() bool {
	return enry.IsVendor(f.filename)
}

func (f fileInstance) isGenerated(content []byte) bool {
	return enry.IsGenerated(f.filename, content)
}

func (f fileInstance) getLanguage(content []byte) string {
	return enry.GetLanguage(f.filename, content)
}

func (f fileInstance) isIgnoredLanguage(lang string) bool {
	// Ignore anything that's neither markup nor a programming language,
	// similar to what the linguist gem does:
	// https://github.com/github/linguist/blob/v7.20.0/lib/linguist/blob_helper.rb#L378-L387
	return enry.GetLanguageType(lang) != enry.Programming &&
		enry.GetLanguageType(lang) != enry.Markup
}

// IsExcluded returns whether
func (f fileInstance) IsExcluded() bool {
	return f.isDocumentation() || f.isVendored()
}

// DetermineStats determines the size and language of the given file. The
// language will be an empty string when the stats should be omitted from the
// count.
func (f fileInstance) DetermineStats(object gitpipe.CatfileObjectResult) (string, uint64, error) {
	// Read arbitrary number of bytes considered enough to determine language
	content, err := io.ReadAll(io.LimitReader(object, 2048))
	if err != nil {
		return "", 0, fmt.Errorf("determine stats read blob: %w", err)
	}

	if f.isGenerated(content) {
		return "", 0, nil
	}

	lang := f.getLanguage(content)

	if f.isIgnoredLanguage(lang) {
		return "", 0, nil
	}

	return lang, uint64(object.Object.ObjectSize()), nil
}
