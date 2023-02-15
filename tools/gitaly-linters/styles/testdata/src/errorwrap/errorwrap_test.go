package errorwrap

import "fmt"

// This file is the test fixture for Gitaly linters

func errorWrapOkay() {
	err := fmt.Errorf("test error")

	fmt.Printf("error: %s", "something else")
	fmt.Printf("error: %v", "something else")
	fmt.Printf("error: %q", "something else")
	fmt.Printf("error: %s %d", "something else", 5)

	_ = fmt.Errorf("error: %w", err)
	_ = fmt.Errorf("error: %w", fmt.Errorf("error: %s", "hello"))
}

func errorWrapNotOkay() {
	err := fmt.Errorf("test error")

	fmt.Printf("error: %s", err)                      // want "please use %w to wrap errors"
	fmt.Printf("error: %s", fmt.Errorf("test error")) // want "please use %w to wrap errors"
	fmt.Printf("error: %v", err)                      // want "please use %w to wrap errors"
	fmt.Printf("error: %v", fmt.Errorf("test error")) // want "please use %w to wrap errors"
	fmt.Printf("error: %q", err)                      // want "please use %w to wrap errors"
	fmt.Printf("error: %q", fmt.Errorf("test error")) // want "please use %w to wrap errors"
	fmt.Printf("error number %d: %s", 5, err)         // want "please use %w to wrap errors"

	_ = fmt.Errorf("error: %w", err)
	_ = fmt.Errorf("error: %w", fmt.Errorf("error: %s", err)) // want "please use %w to wrap errors"
}
