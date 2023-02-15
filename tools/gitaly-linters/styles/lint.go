package main

import (
	"gitlab.com/gitlab-org/gitaly/tools/gitaly-linters/styles/interpolation"
	"golang.org/x/tools/go/analysis"
)

type analyzerPlugin struct{}

func (*analyzerPlugin) GetAnalyzers() []*analysis.Analyzer {
	return []*analysis.Analyzer{
		interpolation.QuoteInterpolationAnalyzer,
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
