package unavailable_code

import (
	"fmt"
)

func NewUnavailable(msg string) error {
	return fmt.Errorf("unavailable: %s", msg)
}

func NewAborted(msg string) error {
	return fmt.Errorf("aborted: %s", msg)
}

func errorWrapOkay() {
	_ = NewAborted("hello world")
}

func errorWrapNotOkay() {
	_ = NewUnavailable("hello world") // please avoid using the Unavailable status code: https://gitlab.com/gitlab-org/gitaly/-/blob/master/STYLE.md?plain=0#unavailable-code
}
