package main

import (
	"golang.org/x/tools/go/analysis"
)

type analyzerPlugin struct{}

func (*analyzerPlugin) GetAnalyzers() []*analysis.Analyzer {
	return []*analysis.Analyzer{
		NewQuoteInterpolationAnalyzer([]string{
			"fmt.*",
		}),
		NewErrorWrapAnalyzer([]string{
			"fmt.Errorf",
			"gitlab.com/gitlab-org/gitaly/v15/internal/structerr.*",
		}),
	}
}

// AnalyzerPlugin is a convention of golangci-lint to implement a custom linter. This variable
// must implement `AnalyzerPlugin` interface:
//
//	type AnalyzerPlugin interface {
//		GetAnalyzers() []*analysis.Analyzer
//	}
//
// For more information, please visit https://golangci-lint.run/contributing/new-linters/
var AnalyzerPlugin analyzerPlugin
