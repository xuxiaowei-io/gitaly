package interpolation

import (
	"golang.org/x/tools/go/analysis/analysistest"
	"os"
	"path/filepath"
	"testing"
)

func TestQuoteInterpolationAnalyzer(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get wd: %s", err)
	}

	testdata := filepath.Join(filepath.Dir(wd), "testdata")
	analysistest.Run(t, testdata, QuoteInterpolationAnalyzer, "quote")
}
