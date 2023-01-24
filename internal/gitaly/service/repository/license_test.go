//go:build !gitaly_test_sha256

package repository

import (
	"context"
	"os"
	"testing"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	mitLicense = `MIT License

Copyright (c) [year] [fullname]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`
)

func testSuccessfulFindLicenseRequest(t *testing.T, cfg config.Cfg, client gitalypb.RepositoryServiceClient, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets(featureflag.GoFindLicense).Run(t, func(t *testing.T, ctx context.Context) {
		for _, tc := range []struct {
			desc                  string
			nonExistentRepository bool
			setup                 func(t *testing.T, repoPath string)
			// expectedLicenseRuby is used to verify the response received from the Ruby side-car.
			// Also is it used if expectedLicenseGo is not set. Because the Licensee gem and
			// the github.com/go-enry/go-license-detector go package use different license databases
			// and different methods to detect the license, they will not always return the
			// same result. So we need to provide different expected results in some cases.
			expectedLicenseRuby *gitalypb.FindLicenseResponse
			expectedLicenseGo   *gitalypb.FindLicenseResponse
			errorContains       string
		}{
			{
				desc: "repository does not exist",
				setup: func(t *testing.T, repoPath string) {
					require.NoError(t, os.RemoveAll(repoPath))
				},
				errorContains: "GetRepoPath: not a git repository",
			},
			{
				desc: "empty if no license file in repo",
				setup: func(t *testing.T, repoPath string) {
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "README.md",
								Content: "readme content",
							}))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{},
			},
			{
				desc: "high confidence mit result and less confident mit-0 result",
				setup: func(t *testing.T, repoPath string) {
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "LICENSE",
								Content: mitLicense,
							}))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{
					LicenseShortName: "mit",
					LicenseUrl:       "http://choosealicense.com/licenses/mit/",
					LicenseName:      "MIT License",
					LicensePath:      "LICENSE",
				},
				expectedLicenseGo: &gitalypb.FindLicenseResponse{
					LicenseShortName: "mit",
					LicenseUrl:       "https://opensource.org/licenses/MIT",
					LicenseName:      "MIT License",
					LicensePath:      "LICENSE",
				},
			},
			{
				// test for https://gitlab.com/gitlab-org/gitaly/-/issues/4745
				desc: "ignores licenses that don't have further details",
				setup: func(t *testing.T, repoPath string) {
					licenseText := testhelper.MustReadFile(t, "testdata/linux-license.txt")

					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "COPYING",
								Content: string(licenseText),
							}))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{
					LicenseShortName: "other",
					LicenseName:      "Other",
					LicenseNickname:  "LICENSE",
					LicensePath:      "COPYING",
				},
				expectedLicenseGo: &gitalypb.FindLicenseResponse{
					LicenseShortName: "gpl-2.0+",
					LicenseName:      "GNU General Public License v2.0 or later",
					LicenseUrl:       "https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html",
					LicensePath:      "COPYING",
				},
			},
			{
				desc: "unknown license",
				setup: func(t *testing.T, repoPath string) {
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "LICENSE.md",
								Content: "this doesn't match any known license",
							}))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{
					LicenseShortName: "other",
					LicenseName:      "Other",
					LicenseNickname:  "LICENSE",
					LicensePath:      "LICENSE.md",
				},
				expectedLicenseGo: &gitalypb.FindLicenseResponse{
					LicenseShortName: "other",
					LicenseName:      "Other",
					LicenseNickname:  "LICENSE",
					LicensePath:      "LICENSE.md",
				},
			},
			{
				desc: "deprecated license",
				setup: func(t *testing.T, repoPath string) {
					deprecatedLicenseData := testhelper.MustReadFile(t, "testdata/gnu_license.deprecated.txt")

					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "LICENSE",
								Content: string(deprecatedLicenseData),
							}))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{
					LicenseShortName: "gpl-3.0",
					LicenseUrl:       "http://choosealicense.com/licenses/gpl-3.0/",
					LicenseName:      "GNU General Public License v3.0",
					LicensePath:      "LICENSE",
					LicenseNickname:  "GNU GPLv3",
				},
				expectedLicenseGo: &gitalypb.FindLicenseResponse{
					LicenseShortName: "gpl-3.0+",
					LicenseUrl:       "https://www.gnu.org/licenses/gpl-3.0-standalone.html",
					LicenseName:      "GNU General Public License v3.0 or later",
					LicensePath:      "LICENSE",
					// The nickname is not set because there is no nickname defined for gpl-3.0+ license.
				},
			},
			{
				desc: "license with nickname",
				setup: func(t *testing.T, repoPath string) {
					licenseText := testhelper.MustReadFile(t, "testdata/gpl-2.0_license.txt")

					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "LICENSE",
								Content: string(licenseText),
							}))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{
					LicenseShortName: "gpl-2.0",
					LicenseUrl:       "http://choosealicense.com/licenses/gpl-2.0/",
					LicenseName:      "GNU General Public License v2.0",
					LicensePath:      "LICENSE",
					LicenseNickname:  "GNU GPLv2",
				},
				expectedLicenseGo: &gitalypb.FindLicenseResponse{
					LicenseShortName: "gpl-2.0",
					LicenseUrl:       "https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html",
					LicenseName:      "GNU General Public License v2.0 only",
					LicensePath:      "LICENSE",
					LicenseNickname:  "GNU GPLv2",
				},
			},
			{
				desc: "license in subdir",
				setup: func(t *testing.T, repoPath string) {
					subTree := gittest.WriteTree(t, cfg, repoPath,
						[]gittest.TreeEntry{{
							Mode:    "100644",
							Path:    "LICENSE",
							Content: mitLicense,
						}})

					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode: "040000",
								Path: "legal",
								OID:  subTree,
							}))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{},
			},
			{
				desc: "license pointing to license file",
				setup: func(t *testing.T, repoPath string) {
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
						gittest.WithTreeEntries(
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "mit.txt",
								Content: mitLicense,
							},
							gittest.TreeEntry{
								Mode:    "100644",
								Path:    "LICENSE",
								Content: "mit.txt",
							},
						))
				},
				expectedLicenseRuby: &gitalypb.FindLicenseResponse{
					LicenseShortName: "other",
					LicenseName:      "Other",
					LicenseNickname:  "LICENSE",
					LicensePath:      "LICENSE",
				},
				expectedLicenseGo: &gitalypb.FindLicenseResponse{
					LicenseShortName: "mit",
					LicenseUrl:       "https://opensource.org/licenses/MIT",
					LicenseName:      "MIT License",
					LicensePath:      "mit.txt",
				},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
				tc.setup(t, repoPath)

				if _, err := os.Stat(repoPath); !os.IsNotExist(err) {
					gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/main")
				}

				resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})
				if tc.errorContains != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errorContains)
					return
				}

				require.NoError(t, err)
				if featureflag.GoFindLicense.IsEnabled(ctx) && tc.expectedLicenseGo != nil {
					testhelper.ProtoEqual(t, tc.expectedLicenseGo, resp)
				} else {
					testhelper.ProtoEqual(t, tc.expectedLicenseRuby, resp)
				}
			})
		}
	})
}

func testFindLicenseRequestEmptyRepo(t *testing.T, cfg config.Cfg, client gitalypb.RepositoryServiceClient, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets(featureflag.GoFindLicense).Run(t, func(t *testing.T, ctx context.Context) {
		repo, _ := gittest.CreateRepository(t, ctx, cfg)

		resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})
		require.NoError(t, err)

		require.Empty(t, resp.GetLicenseShortName())
	})
}

func TestFindLicense_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	client, serverSocketPath := runRepositoryService(t, cfg, nil)
	cfg.SocketPath = serverSocketPath
	_, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: nil})
	msg := testhelper.GitalyOrPraefect("empty Repository", "repo scoped: empty Repository")
	testhelper.RequireGrpcError(t, status.Error(codes.InvalidArgument, msg), err)
}

func BenchmarkFindLicense(b *testing.B) {
	cfg := testcfg.Build(b)
	ctx := testhelper.Context(b)
	ctx = featureflag.ContextWithFeatureFlag(ctx, featureflag.GoFindLicense, true)

	gitCmdFactory := gittest.NewCountingCommandFactory(b, cfg)
	client, serverSocketPath := runRepositoryService(
		b,
		cfg,
		nil,
		testserver.WithGitCommandFactory(gitCmdFactory),
	)
	cfg.SocketPath = serverSocketPath

	// Warm up the license database
	licensedb.Preload()

	repoGitLab, _ := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   "benchmark.git",
	})

	repoStress, repoStressPath := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	// Based on https://github.com/go-enry/go-license-detector/blob/18a439e5437cd46905b074ac24c27cbb6cac4347/licensedb/internal/investigation.go#L28-L38
	fileNames := []string{
		"licence",
		"lisence", //nolint:misspell
		"lisense", //nolint:misspell
		"license",
		"licences",
		"lisences",
		"lisenses",
		"licenses",
		"legal",
		"copyleft",
		"copyright",
		"copying",
		"unlicense",
		"gpl-v1",
		"gpl-v2",
		"gpl-v3",
		"lgpl-v1",
		"lgpl-v2",
		"lgpl-v3",
		"bsd",
		"mit",
		"apache",
	}
	fileExtensions := []string{
		"",
		".md",
		".rst",
		".html",
		".txt",
	}

	treeEntries := make([]gittest.TreeEntry, 0, len(fileNames)*len(fileExtensions))

	for _, name := range fileNames {
		for _, ext := range fileExtensions {
			treeEntries = append(treeEntries,
				gittest.TreeEntry{
					Mode:    "100644",
					Path:    name + ext,
					Content: mitLicense + "\n" + name, // grain of salt
				})
		}
	}

	gittest.WriteCommit(b, cfg, repoStressPath, gittest.WithBranch("main"),
		gittest.WithTreeEntries(treeEntries...))
	gittest.Exec(b, cfg, "-C", repoStressPath, "symbolic-ref", "HEAD", "refs/heads/main")

	testhelper.NewFeatureSets(featureflag.LocalrepoReadObjectCached).Bench(b, func(b *testing.B, ctx context.Context) {
		ctx = featureflag.ContextWithFeatureFlag(ctx, featureflag.GoFindLicense, true)
		ctx = correlation.ContextWithCorrelation(ctx, "1")
		ctx = testhelper.MergeOutgoingMetadata(ctx,
			metadata.Pairs(catfile.SessionIDField, "1"),
		)

		for _, tc := range []struct {
			desc string
			repo *gitalypb.Repository
		}{
			{
				desc: "gitlab-org/gitlab.git",
				repo: repoGitLab,
			},
			{
				desc: "stress.git",
				repo: repoStress,
			},
		} {
			b.Run(tc.desc, func(b *testing.B) {
				gitCmdFactory.ResetCount()

				for i := 0; i < b.N; i++ {
					resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: tc.repo})
					require.NoError(b, err)
					require.Equal(b, "mit", resp.GetLicenseShortName())
				}

				if featureflag.LocalrepoReadObjectCached.IsEnabled(ctx) {
					catfileCount := gitCmdFactory.CommandCount("cat-file")
					require.LessOrEqual(b, catfileCount, uint64(1))
				}
			})
		}
	})
}
