package cfgerror

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestValidationError_Error(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", ValidationError{}.Error())
	require.Equal(t, "1.2", ValidationError{Key: []string{"1", "2"}}.Error())
	require.Equal(t, "err", ValidationError{Cause: errors.New("err")}.Error())
	require.Equal(t, "1.2: err", ValidationError{
		Key:   []string{"1", "2"},
		Cause: errors.New("err"),
	}.Error())
}

func TestValidationErrors_Append(t *testing.T) {
	t.Parallel()

	err1 := NewValidationError(errors.New("bad-1"), "added")

	t.Run("add nil", func(t *testing.T) {
		var errs ValidationErrors
		require.Equal(t, New(), errs.Append(nil, "some"))
	})

	t.Run("add ValidationError type", func(t *testing.T) {
		var errs ValidationErrors
		require.Equal(t, ValidationErrors{err1}, errs.Append(err1))

		require.Equal(t, ValidationErrors{
			{
				Cause: err1.Cause,
				Key:   append([]string{"2"}, err1.Key...),
			},
		}, errs.Append(err1, "2"))
	})

	t.Run("add ValidationErrors type", func(t *testing.T) {
		err2 := NewValidationError(errors.New("bad-2"), "nested")
		err3 := NewValidationError(errors.New("bad-2"), "root", "outer", "nested")

		var errs ValidationErrors
		require.Equal(t, ValidationErrors{err1}, errs.Append(ValidationErrors{err1}))

		require.Equal(t, ValidationErrors{err3}, errs.Append(ValidationErrors{err2}, "root", "outer"))
	})

	t.Run("add not ValidationError(s) type", func(t *testing.T) {
		var errs ValidationErrors
		expected := ValidationErrors{{Cause: assert.AnError, Key: []string{"any"}}}
		require.Equal(t, expected, errs.Append(assert.AnError, "any"))
	})
}

func TestValidationErrors_AsError(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		err := ValidationErrors{}.AsError()
		require.NoError(t, err)
	})

	t.Run("non empty", func(t *testing.T) {
		err := ValidationErrors{NewValidationError(errors.New("msg"), "err")}.AsError()
		require.Equal(t, ValidationErrors{{Key: []string{"err"}, Cause: errors.New("msg")}}, err)
	})
}

func TestValidationErrors_Error(t *testing.T) {
	t.Parallel()

	t.Run("serialized", func(t *testing.T) {
		require.Equal(t, "1.2: msg1\n1: msg2",
			ValidationErrors{
				NewValidationError(errors.New("msg1"), "1", "2"),
				NewValidationError(errors.New("msg2"), "1"),
			}.Error(),
		)
	})

	t.Run("nothing to present", func(t *testing.T) {
		require.Equal(t, "", ValidationErrors{}.Error())
	})
}

func TestNewValidationError(t *testing.T) {
	t.Parallel()

	err := NewValidationError(assert.AnError)
	require.Equal(t, ValidationError{Cause: assert.AnError}, err)

	err = NewValidationError(assert.AnError, "outer", "inner")
	require.Equal(t, ValidationError{Cause: assert.AnError, Key: []string{"outer", "inner"}}, err)
}

func TestNotEmpty(t *testing.T) {
	t.Parallel()
	require.NoError(t, NotEmpty("value"))
	require.Equal(t, NewValidationError(ErrNotSet), NotEmpty(""))
}

func TestNotBlank(t *testing.T) {
	t.Parallel()
	require.NoError(t, NotBlank("value"))
	require.Equal(t, NewValidationError(ErrBlankOrEmpty), NotBlank(""))
	require.Equal(t, NewValidationError(ErrBlankOrEmpty), NotBlank("  \t  \n "))
}

func TestDirExists(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(testhelper.TempDir(t), "tmp-file")
	require.NoError(t, os.WriteFile(filePath, []byte{}, perm.PublicFile))
	existing := testhelper.TempDir(t)
	notExisting := filepath.Join(existing, "bad")

	require.NoError(t, DirExists(existing))

	expectedNotExisting := NewValidationError(fmt.Errorf("%w: %q", ErrDoesntExist, notExisting))
	require.Equal(t, expectedNotExisting, DirExists(notExisting))

	expectedNotDir := NewValidationError(fmt.Errorf("%w: %q", ErrNotDir, filePath))
	require.Equal(t, expectedNotDir, DirExists(filePath))
}

func TestFileExists(t *testing.T) {
	t.Parallel()

	dir := testhelper.TempDir(t)
	existing := filepath.Join(dir, "tmp-file")
	require.NoError(t, os.WriteFile(existing, []byte{}, perm.PublicFile))
	notExisting := filepath.Join(dir, "bad")

	require.NoError(t, FileExists(existing))

	expectedNotExisting := NewValidationError(fmt.Errorf("%w: %q", ErrDoesntExist, notExisting))
	require.Equal(t, expectedNotExisting, FileExists(notExisting))

	expectedNotFile := NewValidationError(fmt.Errorf("%w: %q", ErrNotFile, dir))
	require.Equal(t, expectedNotFile, FileExists(dir))
}

func TestIsPositive(t *testing.T) {
	t.Parallel()
	require.NoError(t, IsPositive(0))
	require.NoError(t, IsPositive(100))
	require.Equal(t, NewValidationError(fmt.Errorf("%w: -1.2", ErrIsNegative)), IsPositive(-1.2))
}
