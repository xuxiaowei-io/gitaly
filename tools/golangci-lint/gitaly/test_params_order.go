package main

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

const testParamsOrder = "test_params_order"

// newTestParamsOrder returns an analyzer to detect parameters of test helper functions. testing.TB arguments should
// always be passed as first parameter, followed by context.Context.
//
//   - Bad
//     func testHelper(paramA string, t *testing.T)
//
//   - Bad
//     func testHelper(ctx context.Context, t *testing.T)
//
//   - Bad
//     func testHelper(t *testing.T, paramA string, ctx context.Context)
//
//   - Good
//     func testHelper(t *testing.T)
//
//   - Good
//     func testHelper(t *testing.T, ctx context.Context)
//
//   - Good
//     func testHelper(t *testing.T, ctx context.Context, paramA string)
//
// For more information:
// https://gitlab.com/gitlab-org/gitaly/-/blob/master/STYLE.md?ref_type=heads#test-helpers
func newTestParamsOrder() *analysis.Analyzer {
	return &analysis.Analyzer{
		Name: testParamsOrder,
		Doc:  `testing.TB arguments should always be passed as first parameter, followed by context.Context if required: https://gitlab.com/gitlab-org/gitaly/-/blob/master/STYLE.md?ref_type=heads#test-helpers`,
		Run:  runTestParamsOrder,
	}
}

var (
	testingTB      = mustFindPackageInterface("testing", "TB")
	contextContext = mustFindPackageInterface("context", "Context")
)

func runTestParamsOrder(pass *analysis.Pass) (interface{}, error) {
	for _, file := range pass.Files {
		ast.Inspect(file, func(n ast.Node) bool {
			if decl, ok := n.(*ast.FuncDecl); ok {
				analyzeTestHelperParams(pass, decl)
			}
			return true
		})
	}
	return nil, nil
}

func analyzeTestHelperParams(pass *analysis.Pass, decl *ast.FuncDecl) {
	params := decl.Type.Params
	// Either case is fine:
	// - No param. Out of scope.
	// - The only param is not testing.TB. Out of scope.
	// - The only param is testing.TB. This is perfectly fine.
	if params.NumFields() <= 1 {
		return
	}

	testingTBIndex := -1
	contextContextIndex := -1
	for index, field := range params.List {
		fieldType := pass.TypesInfo.TypeOf(field.Type)
		if types.Implements(fieldType, testingTB) {
			// More than one testing.TB parameters
			if testingTBIndex != -1 {
				pass.Report(analysis.Diagnostic{
					Pos:     params.Pos(),
					End:     params.End(),
					Message: "more than one testing.TB parameter",
				})
				return
			}
			testingTBIndex = index
		}
		if fieldType.Underlying().String() == contextContext.String() {
			contextContextIndex = index
		}
	}

	switch {
	case testingTBIndex == -1:
		// No testing.TB parameter is present. The function is probably not a test helper function.
		return
	case testingTBIndex != 0:
		testingTBField := params.List[testingTBIndex]
		pass.Report(analysis.Diagnostic{
			Pos:     testingTBField.Pos(),
			End:     testingTBField.End(),
			Message: "testing.TB argument should always be passed as first parameter",
		})
	case contextContextIndex != -1 && contextContextIndex != 1:
		testingTBField := params.List[testingTBIndex]
		pass.Report(analysis.Diagnostic{
			Pos:     testingTBField.Pos(),
			End:     testingTBField.End(),
			Message: "context.Context should follow after testing.TB",
		})
	}
}
