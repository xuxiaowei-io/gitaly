package linguist

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/go-enry/go-enry/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
)

// ByteCountPerLanguage represents a counter value (bytes) per language.
type ByteCountPerLanguage map[string]uint64

// Instance is a holder of the defined in the system language settings.
type Instance struct {
	cfg config.Cfg
}

// New loads the name->color map from the Linguist gem and returns initialized
// instance to use back to the caller or an error.
func New(cfg config.Cfg) (*Instance, error) {
	return &Instance{
		cfg: cfg,
	}, nil
}

// Stats returns the repository's language stats as reported by 'git-linguist'.
func (inst *Instance) Stats(ctx context.Context, repo *localrepo.Repo, commitID string, catfileCache catfile.Cache) (ByteCountPerLanguage, error) {
	if featureflag.GoLanguageStats.IsEnabled(ctx) {
		return inst.enryStats(ctx, repo, commitID, catfileCache)
	}

	repoPath, err := repo.Path()
	if err != nil {
		return nil, fmt.Errorf("get repo path: %w", err)
	}

	cmd, err := inst.startGitLinguist(ctx, repoPath, commitID)
	if err != nil {
		return nil, fmt.Errorf("starting linguist: %w", err)
	}

	data, err := io.ReadAll(cmd)
	if err != nil {
		return nil, fmt.Errorf("reading linguist output: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("waiting for linguist: %w", err)
	}

	stats := make(ByteCountPerLanguage)
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("unmarshaling stats: %w", err)
	}

	return stats, nil
}

// Color returns the color Linguist has assigned to language.
func Color(language string) string {
	if color := enry.GetColor(language); color != "#cccccc" {
		return color
	}

	colorSha := sha256.Sum256([]byte(language))
	return fmt.Sprintf("#%x", colorSha[0:3])
}

func (inst *Instance) startGitLinguist(ctx context.Context, repoPath string, commitID string) (*command.Command, error) {
	bundle, err := exec.LookPath("bundle")
	if err != nil {
		return nil, fmt.Errorf("finding bundle executable: %w", err)
	}

	cmd := []string{bundle, "exec", "bin/gitaly-linguist", "--repository=" + repoPath, "--commit=" + commitID}

	internalCmd, err := command.New(ctx, cmd,
		command.WithDir(inst.cfg.Ruby.Dir),
		command.WithEnvironment(env.AllowedRubyEnvironment(os.Environ())),
		command.WithCommandName("git-linguist", "stats"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating command: %w", err)
	}

	return internalCmd, nil
}

func (inst *Instance) enryStats(ctx context.Context, repo *localrepo.Repo, commitID string, catfileCache catfile.Cache) (ByteCountPerLanguage, error) {
	stats, err := newLanguageStats(repo)
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Info("linguist load from cache")
	}
	if stats.CommitID == commitID {
		return stats.Totals, nil
	}

	objectReader, cancel, err := catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("create object reader: %w", err)
	}
	defer cancel()

	var revlistIt gitpipe.RevisionIterator

	if stats.CommitID == "" {
		// No existing stats cached, so get all the files for the commit
		// using git-ls-tree(1).
		revlistIt = gitpipe.LsTree(ctx, repo,
			commitID,
			gitpipe.LsTreeWithRecursive(),
			gitpipe.LsTreeWithBlobFilter(),
		)
	} else {
		// Stats are cached for one commit, so get the git-diff-tree(1)
		// between that commit and the one we're calculating stats for.

		skipDeleted := func(result *gitpipe.RevisionResult) bool {
			// Skip files that are deleted.
			if git.ObjectHashSHA1.IsZeroOID(result.OID) {
				// It's a little bit of a hack to use this skip
				// function, but for every file that's deleted,
				// remove the stats.
				stats.drop(string(result.ObjectName))
				return true
			}
			return false
		}

		revlistIt = gitpipe.DiffTree(ctx, repo,
			stats.CommitID, commitID,
			gitpipe.DiffTreeWithRecursive(),
			gitpipe.DiffTreeWithIgnoreSubmodules(),
			gitpipe.DiffTreeWithSkip(skipDeleted),
		)
	}

	objectIt, err := gitpipe.CatfileObject(ctx, objectReader, revlistIt)
	if err != nil {
		return nil, fmt.Errorf("linguist gitpipe: %w", err)
	}

	for objectIt.Next() {
		object := objectIt.Result()
		filename := string(object.ObjectName)

		// Read arbitrary number of bytes considered enough to determine language
		content, err := io.ReadAll(io.LimitReader(object, 2048))
		if err != nil {
			return nil, fmt.Errorf("linguist read blob: %w", err)
		}

		if _, err := io.Copy(io.Discard, object); err != nil {
			return nil, fmt.Errorf("linguist discard excess blob: %w", err)
		}

		lang := enry.GetLanguage(filename, content)

		// Ignore anything that's neither markup nor a programming language,
		// similar to what the linguist gem does:
		// https://github.com/github/linguist/blob/v7.20.0/lib/linguist/blob_helper.rb#L378-L387
		if enry.GetLanguageType(lang) != enry.Programming &&
			enry.GetLanguageType(lang) != enry.Markup {
			// The file might have been included in the stats before
			stats.drop(filename)

			continue
		}

		stats.add(filename, lang, uint64(object.Object.ObjectSize()))
	}

	if err := objectIt.Err(); err != nil {
		return nil, fmt.Errorf("linguist object iterator: %w", err)
	}

	if err := stats.save(repo, commitID); err != nil {
		return nil, fmt.Errorf("linguist language stats save: %w", err)
	}

	return stats.Totals, nil
}
