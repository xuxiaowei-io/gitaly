package main

import (
	"strings"

	"github.com/spf13/viper"
	"golang.org/x/tools/go/analysis"
)

type analyzerPlugin struct{}

func (p *analyzerPlugin) GetAnalyzers() []*analysis.Analyzer {
	return []*analysis.Analyzer{
		newQuoteInterpolationAnalyzer(&quoteInterpolationAnalyzerSettings{
			IncludedFunctions: p.configStringSlicesAt(
				quoteInterpolationAnalyzerName,
				"included-functions",
			),
		}),
		newErrorWrapAnalyzer(&errorWrapAnalyzerSettings{
			IncludedFunctions: p.configStringSlicesAt(
				errorWrapAnalyzerName,
				"included-functions",
			),
		}),
	}
}

// This method fetches a string slices in golangci-lint config files for the input analyzer. This is
// an enhancement to golangci-lint. Although it supports custom linters, it doesn't support parsing
// custom settings for such linters. We have to take care of parsing ourselves. Fortunately,
// golangci-lint uses `viper` package underlying. This package maintains a global process state.
// This state stores the parsed configurations of all custom linters. As custom linter is loaded
// after all other public linters, it's guaranteed that viper state is already established.
//
// It's true this behavior may change in the future, but it's still better than reading and parsing
// the config file ourselves. We may consider that approach if this way doesn't work
//
// # The structure for custom linter's settings is described as followed:
//
// ```yaml
//
//	   linters:
//		custom:
//		  gitaly-linters:
//		    path: ./_build/tools/gitaly-linters.so
//		    description: A collection of linters tailored for Gitaly
//		    original-url: gitlab.com/gitlab-org/gitaly
//		    settings:
//		      string_interpolation_quote:
//		        included-functions:
//		          - fmt.*
//		      error_wrap:
//		        included-functions:
//		          - fmt.Errorf
//		          - gitlab.com/gitlab-org/gitaly/v15/internal/structerr.*
//
// ```
func (*analyzerPlugin) configStringSlicesAt(analyzer string, key string) []string {
	path := strings.Join([]string{
		"linters-settings",
		"custom",
		"gitaly-linters",
		"settings",
		analyzer,
		key,
	}, ".")
	return viper.GetStringSlice(path)
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
