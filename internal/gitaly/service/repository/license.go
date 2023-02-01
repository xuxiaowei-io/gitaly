package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	classifier "github.com/google/licenseclassifier/v2"
	classifierassets "github.com/google/licenseclassifier/v2/assets"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/unarycache"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// The `github.com/go-enry/go-license-detector` package uses https://spdx.org/licenses/
// as the source of the licenses. That package doesn't provide `nickname` info.
// But because the `nickname` is required by the FindLicense RPC interface, we had to manually
// extract the list of all license-to-nickname pairs from the Licensee license
// database which is https://github.com/github/choosealicense.com/tree/gh-pages/_licenses
// and store them here.
var nicknameByLicenseIdentifier = map[string]string{
	"agpl-3.0":           "GNU AGPLv3",
	"lgpl-3.0":           "GNU LGPLv3",
	"bsd-3-clause-clear": "Clear BSD",
	"odbl-1.0":           "ODbL",
	"ncsa":               "UIUC/NCSA",
	"lgpl-2.1":           "GNU LGPLv2.1",
	"gpl-3.0":            "GNU GPLv3",
	"gpl-2.0":            "GNU GPLv2",
}

var globalClassifier *classifier.Classifier

// PreloadLicenseDatabase will warm up the license database.
func PreloadLicenseDatabase() error {
	var err error
	globalClassifier, err = classifierassets.DefaultClassifier()
	if err != nil {
		return err
	}

	log.Info("License database preloaded")

	return nil
}

func newLicenseCache() *unarycache.Cache[git.ObjectID, *gitalypb.FindLicenseResponse] {
	cache, err := unarycache.New(100, findLicense)
	if err != nil {
		panic(err)
	}
	return cache
}

func (s *server) FindLicense(ctx context.Context, req *gitalypb.FindLicenseRequest) (*gitalypb.FindLicenseResponse, error) {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	if featureflag.GoFindLicense.IsEnabled(ctx) {
		repo := localrepo.New(s.locator, s.gitCmdFactory, s.catfileCache, repository)

		headOID, err := repo.ResolveRevision(ctx, "HEAD")
		if err != nil {
			if errors.Is(err, git.ErrReferenceNotFound) {
				return &gitalypb.FindLicenseResponse{}, nil
			}
			return nil, structerr.NewInternal("cannot find HEAD revision: %v", err)
		}

		response, err := s.licenseCache.GetOrCompute(ctx, repo, headOID)
		if err != nil {
			return nil, err
		}

		return response, nil
	}

	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}
	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, repository)
	if err != nil {
		return nil, err
	}
	return client.FindLicense(clientCtx, req)
}

func findLicense(ctx context.Context, repo *localrepo.Repo, commitID git.ObjectID) (*gitalypb.FindLicenseResponse, error) {
	repoFiler := &gitFiler{ctx: ctx, repo: repo, treeishID: commitID}

	files, err := repoFiler.ReadDir("")
	if err != nil {
		return nil, fmt.Errorf("read root dir: %w", err)
	}

	objectReader, cancel, err := repo.ObjectReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("create object reader: %w", err)
	}
	defer cancel()

	var matches classifier.Matches
	for _, f := range files {
		rev := git.Revision(fmt.Sprintf("%s:%s", commitID, f))
		// oid := git.ObjectID(rev.String())

		// data, err := repo.ReadObject(ctx, oid)
		// if err != nil {
		// 	return nil, fmt.Errorf("read object %v: %w", rev.String(), err)
		// }
		// results := globalClassifier.Match(data)

		object, err := objectReader.Object(ctx, rev)
		if err != nil {
			return nil, fmt.Errorf("read file %v: %w", f, err)
		}
		results, err := globalClassifier.MatchFrom(object)
		if err != nil {
			return nil, fmt.Errorf("classify %v: %w", f, err)
		}
		matches = append(matches, results.Matches...)
	}
	sort.Slice(matches, func(i, j int) bool {
		// Because there could be multiple matches with the same confidence, we need
		// to make sure the function is consistent and returns the same license on
		// each invocation. That is why we sort by the short name as well.
		if matches[i].Confidence == matches[j].Confidence {
			return matches[i].Name < matches[j].Name
		}
		return matches[i].Confidence > matches[j].Confidence
	})
	if len(matches) == 0 {
		return &gitalypb.FindLicenseResponse{}, nil
	}

	bestMatch := matches[0]

	// TODO https://github.com/spdx/license-list-data/tree/main/json
	return &gitalypb.FindLicenseResponse{
		LicenseShortName: bestMatch.Name,
		LicensePath:      bestMatch.Name,
		LicenseName:      bestMatch.Name,
		LicenseUrl:       bestMatch.Name,
		LicenseNickname:  bestMatch.Name,
	}, nil
}

type gitFiler struct {
	ctx       context.Context
	repo      *localrepo.Repo
	treeishID git.ObjectID
}

func (f *gitFiler) ReadDir(string) ([]string, error) {
	var stderr bytes.Buffer
	cmd, err := f.repo.Exec(f.ctx, git.Command{
		Name: "ls-tree",
		Flags: []git.Option{
			//git.Flag{Name: "--full-tree"},
			git.Flag{Name: "-z"},
		},
		Args: []string{f.treeishID.String()},
	}, git.WithStderr(&stderr))
	if err != nil {
		return nil, err
	}

	tree := lstree.NewParser(cmd, git.ObjectHashSHA1)

	var files []string
	for {
		entry, err := tree.NextEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if entry.Type != lstree.Blob {
			continue
		}

		files = append(files, entry.Path)
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("ls-tree failed: %w, stderr: %q", err, stderr.String())
	}

	return files, nil
}
