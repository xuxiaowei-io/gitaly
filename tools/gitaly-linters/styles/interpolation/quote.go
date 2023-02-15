package interpolation

import (
	"go/ast"
	"go/token"
	"golang.org/x/tools/go/analysis"
	"regexp"
	"strings"
)

// QuoteInterpolationAnalyzer implements an analyzer to detect manually quoted string interpolation
// with '%s' and "%s". Quoting this way doesn't escape special characters such as endline and makes
// debugging harder later. We encourage to use %q verb instead.
//
//   - Bad
//     return fmt.Errorf("gl_id='%s' is invalid", glID)
//
//   - Bad
//     fmt.Sprintf("fatal: not a git repository: '%s'", repoPath)
//
//   - Good
//     return fmt.Errorf("gl_id=%q is invalid", glID)
//
//   - Good
//     fmt.Sprintf("fatal: not a git repository: %q", repoPath)
//
// For more information:
// https://gitlab.com/gitlab-org/gitaly/-/blob/master/STYLE.md#use-q-when-interpolating-strings
var QuoteInterpolationAnalyzer = &analysis.Analyzer{
	Name: "string_interpolation_quote",
	Doc: `Unless it would lead to incorrect results, always use %q when
	interpolating strings. For more information:
	https://gitlab.com/gitlab-org/gitaly/-/blob/master/STYLE.md#use-q-when-interpolating-strings`,
	Run: runStringInterpolationQuoteAnalyzer,
}

// offendedFormatPattern matches string interpolation having '%s' and "%s" format
var offendedFormatPattern = regexp.MustCompile(`['"]%s['"]`)

func analyzeInterpolation(str *ast.BasicLit, pass *analysis.Pass) {
	value := str.Value
	if strings.HasPrefix(value, `'`) || strings.HasPrefix(value, `"`) {
		value = value[1:]
	}
	if strings.HasSuffix(value, `'`) || strings.HasSuffix(value, `"`) {
		value = value[:len(value)-1]
	}
	for _, index := range offendedFormatPattern.FindAllIndex([]byte(value), -1) {
		start := token.Pos(int(str.Pos()) + index[0] + 1)
		end := token.Pos(int(str.Pos()) + index[1])
		pass.Report(analysis.Diagnostic{
			Pos:            start,
			End:            end,
			Message:        "wrapping %s verb with quotes is not encouraged, please use %q instead",
			SuggestedFixes: nil,
		})
	}
}

func runStringInterpolationQuoteAnalyzer(pass *analysis.Pass) (interface{}, error) {
	for _, file := range pass.Files {
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if len(call.Args) >= 1 {
					if str, ok := call.Args[0].(*ast.BasicLit); ok && str.Kind == token.STRING {
						analyzeInterpolation(str, pass)
					}
				}
			}
			return true
		})
	}
	return nil, nil
}
