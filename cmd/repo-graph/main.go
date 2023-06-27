package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("error: %q", err)
	}
}

func Main() error {
	cmd := exec.Command("git", "for-each-ref", "--format", "%(refname)%00%(objectname)")

	refList, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("list refs: %w", err)
	}

	refs := map[string]string{}
	for _, line := range strings.Split(string(refList), "\n") {
		if line == "" {
			break
		}

		components := strings.Split(line, "\x00")
		refs[components[0]] = components[1]
	}

	cmd = exec.Command("git", "cat-file", "--batch-check=%(objectname) %(objecttype)", "--batch-all-objects")
	objectList, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("list objects: %w", err)
	}

	objects := map[string]string{}
	for _, line := range strings.Split(string(objectList), "\n") {
		if line == "" {
			break
		}

		components := strings.Split(line, " ")
		objects[components[0]] = components[1]
	}

	refCSV, err := os.OpenFile("references.csv", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("open reference.csv: %w", err)
	}

	fmt.Fprintf(refCSV, "reference,object_id\n")
	for reference, objectID := range refs {
		fmt.Fprintf(refCSV, "%s,%s\n", reference, objectID)
	}

	repoPath := os.Args[1]
	cfg := config.Cfg{
		Storages: []config.Storage{
			{Name: "default", Path: filepath.Dir(repoPath)},
		},
	}

	repoPb := &gitalypb.Repository{
		StorageName:  "default",
		RelativePath: filepath.Base(repoPath),
	}

	cache := catfile.NewCache(cfg)

	cmdFactory, cleanUp, err := git.NewExecCommandFactory(cfg, git.WithSkipHooks())
	if err != nil {
		return fmt.Errorf("new exec command factory: %w", err)
	}
	defer cleanUp()

	repo := localrepo.New(config.NewLocator(cfg), cmdFactory, cache, repoPb)

	ctx := context.Background()
	objectReader, cleanReader, err := cache.ObjectReader(ctx, repo)
	if err != nil {
		return fmt.Errorf("object reader: %w", err)
	}
	defer cleanReader()

	contentQueue, cleanQueue, err := objectReader.ObjectContentQueue(ctx)
	if err != nil {
		return fmt.Errorf("clean queue: %w", err)
	}
	defer cleanQueue()

	blobCSV := openCSV("blob.csv", "object_id", "content")
	defer blobCSV.Close()
	tagCSV := openCSV("tag.csv", "object_id", "content")
	defer tagCSV.Close()
	commitCSV := openCSV("commit.csv", "object_id", "subject", "message", "tree", "author_name", "author_email", "author_date_epoch", "author_date_tz", "committer_name", "committer_email", "committer_date_epoch", "committer_date_tz")
	defer commitCSV.Close()
	treeCSV := openCSV("tree.csv", "object_id", "content")
	defer treeCSV.Close()

	commitParentsCSV := openCSV("commit_parents.csv", "commit_oid", "parent_oid", "ordinal")
	defer commitParentsCSV.Close()

	treeEntriesCSV := openCSV("tree_entries.csv", "tree_oid", "mode", "entry_oid", "path")
	defer treeEntriesCSV.Close()

	for objectID, objType := range objects {
		if err := contentQueue.RequestObject(ctx, git.Revision(objectID)); err != nil {
			panic(fmt.Errorf("request object: %w", err))
		}
		contentQueue.Flush(ctx)

		object, err := contentQueue.ReadObject(ctx)
		if err != nil {
			return fmt.Errorf("read object: %w", err)
		}

		if object.Oid != git.ObjectID(objectID) {
			return fmt.Errorf("unexpected id")
		}

		switch objType {
		case "commit":
			commit, err := catfile.NewParser().ParseCommit(object)
			if err != nil {
				return fmt.Errorf("parse commit: %w", err)
			}

			fmt.Fprintf(commitCSV, "%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%d,%s\n",
				objectID,
				base64.StdEncoding.EncodeToString(commit.Subject),
				base64.StdEncoding.EncodeToString(commit.Body),
				commit.TreeId,
				base64.StdEncoding.EncodeToString(commit.Author.Name),
				base64.StdEncoding.EncodeToString(commit.Author.Email),
				commit.Author.Date.Seconds,
				fmt.Sprintf("%s:%s", commit.Author.Timezone[:3], commit.Author.Timezone[3:]),
				base64.StdEncoding.EncodeToString(commit.Committer.Name),
				base64.StdEncoding.EncodeToString(commit.Committer.Email),
				commit.Committer.Date.Seconds,
				fmt.Sprintf("%s:%s", commit.Committer.Timezone[:3], commit.Committer.Timezone[3:]),
			)

			for i, parentOID := range commit.ParentIds {
				fmt.Fprintf(commitParentsCSV, "%s,%s,%v\n",
					objectID,
					parentOID,
					i,
				)
			}
		case "tree":
			content, err := ioutil.ReadAll(object)
			if err != nil {
				return fmt.Errorf("read tree: %w", err)
			}

			fmt.Fprintf(treeCSV, "%s,%s\n", objectID, base64.StdEncoding.EncodeToString(content))

			reader := bufio.NewReader(bytes.NewReader(content))
			for {
				modeAndPath, err := reader.ReadString(byte('\x00'))
				if err != nil {
					if err == io.EOF {
						break
					}

					return fmt.Errorf("read mode and path: %q", err)
				}

				components := strings.Split(modeAndPath, " ")
				mode, path := components[0], strings.TrimRight(components[1], "\x00")

				oidBytes := make([]byte, sha1.Size)
				if _, err := reader.Read(oidBytes); err != nil {
					return fmt.Errorf("read oid: %q", err)
				}

				hexOID := hex.EncodeToString(oidBytes)
				fmt.Fprintf(treeEntriesCSV, "%s,%s,%s,%s\n", objectID, mode, hexOID, base64.StdEncoding.EncodeToString([]byte(path)))
			}
		case "tag":
			content, err := ioutil.ReadAll(object)
			if err != nil {
				return fmt.Errorf("read all: %w", err)
			}

			fmt.Fprintf(tagCSV, "%s,%s\n", objectID, base64.StdEncoding.EncodeToString(content))
		case "blob":
			content, err := ioutil.ReadAll(object)
			if err != nil {
				return fmt.Errorf("read all: %w", err)
			}

			fmt.Fprintf(blobCSV, "%s,%s\n", objectID, base64.StdEncoding.EncodeToString(content))
		}
	}

	return nil
}

func openCSV(name string, headers ...string) io.WriteCloser {
	file, err := os.OpenFile(name, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("open %q: %w", name, err))
	}

	if _, err := fmt.Fprintf(file, "%s\n", strings.Join(headers, ",")); err != nil {
		panic(fmt.Errorf("write headers: %w", err))
	}

	return file
}
