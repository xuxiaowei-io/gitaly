package archive

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"golang.org/x/exp/slices"
)

const (
	// readDirEntriesPageSize is an amount of fs.DirEntry(s) to read
	// from the opened file descriptor of the directory.
	readDirEntriesPageSize = 32

	// Below is a set of flags used to decide what to do with the file/directory
	// found on the disk.
	// decisionWrite means that the file/directory needs to be added to the archive
	decisionWrite = iota
	// decisionSkip means that the file/directory shouldn't be added to the archive
	decisionSkip
	// decisionStop means that nothing else left to be added to the archive
	decisionStop

	// tarFormat is a format to use for the archived files/directories/etc.
	// The decision is made to use it as it is the latest version of the format.
	// It allows sparse files, long file names, etc. that older formats doesn't support.
	tarFormat = tar.FormatPAX
)

type decider func(string) int

// WriteTarball writes a tarball to an `io.Writer` for the provided path
// containing the specified archive members. Members should be specified
// relative to `path`.
func WriteTarball(ctx context.Context, logger log.Logger, writer io.Writer, path string, members ...string) error {
	cmdArgs := []string{"-c", "-f", "-", "-C", path}

	if runtime.GOOS == "darwin" {
		cmdArgs = append(cmdArgs, "--no-mac-metadata")
	}

	cmdArgs = append(cmdArgs, members...)

	cmd, err := command.New(ctx, logger, append([]string{"tar"}, cmdArgs...), command.WithStdout(writer))
	if err != nil {
		return fmt.Errorf("executing tar command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("waiting for tar command completion: %w", err)
	}

	return nil
}

// writeTarball creates a tar archive by flushing data into writer.
// Files and folders to be included into archive needs to be provided as members.
// If entry from members slice points to a folder all content inside it will be archived.
// The empty folders will be archived as well.
// It doesn't support sparse files efficiently, see https://github.com/golang/go/issues/22735
func writeTarball(ctx context.Context, writer io.Writer, path string, members ...string) error {
	// The function decides what to do with the entry: write, skip or finish archive
	// creation if all members are added.
	decide := func(candidate string) int {
		if len(members) == 0 {
			return decisionStop
		}

		idx := slices.Index(members, candidate)
		if idx < 0 {
			return decisionSkip
		}

		members = slices.Delete(members, idx, idx+1)
		return decisionWrite
	}

	archWriter := tar.NewWriter(writer)
	if err := walkDir(ctx, archWriter, path, "", decide); err != nil {
		return fmt.Errorf("walk dir: %w", err)
	}

	if err := archWriter.Close(); err != nil {
		return fmt.Errorf("closing archive writer: %w", err)
	}

	return nil
}

func walkDir(ctx context.Context, archWriter *tar.Writer, path, currentPrefix string, decide decider) error {
	if cancelled(ctx) {
		return ctx.Err()
	}

	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer func() { _ = dir.Close() }()

	dirStat, err := dir.Stat()
	if err != nil {
		return fmt.Errorf("dir description: %w", err)
	}

	if dirStat.Mode().Type() != fs.ModeDir {
		return errors.New("is not a dir")
	}

	for {
		// Own implementation of the directory walker is used for optimization.
		// fs.WalkDir() reads all the entries into memory at once and does the sorting.
		// If there are a lot of files in the nested directories all that info will remain
		// in the memory while the leaf is reached. In other words if there is 1000 files
		// in the root and 1000 file in the nested directory of the root in one moment
		// 2000 entries will remain in the memory for no reason.
		dirEntries, err := dir.ReadDir(readDirEntriesPageSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("chunked dir read: %w", err)
		}

		for _, dirEntry := range dirEntries {
			if cancelled(ctx) {
				return ctx.Err()
			}

			entryName := dirEntry.Name()
			entryRelativePath := filepath.Join(currentPrefix, entryName)

			decision := decide(entryRelativePath)
			switch decision {
			case decisionStop:
				// Nothing needs to be added to the archive.
				return nil
			case decisionSkip:
				// Current entry doesn't need to be added, but we need travers it if it is a directory.
				if dirEntry.IsDir() {
					if err := walkDir(ctx, archWriter, filepath.Join(path, entryName), entryRelativePath, decide); err != nil {
						return fmt.Errorf("walk dir: %w", err)
					}
				}
			case decisionWrite:
				if dirEntry.IsDir() {
					// As this is a dir we add all its content to the archive.
					if err := tarDir(ctx, archWriter, filepath.Join(path, entryName), entryRelativePath, func(string) int { return decisionWrite }); err != nil {
						return fmt.Errorf("write dir: %w", err)
					}
					continue
				}

				if err := tarFile(archWriter, path, entryRelativePath); err != nil {
					return fmt.Errorf("write file: %w", err)
				}
			default:
				return fmt.Errorf("unhandled decision: %d", decision)
			}
		}
	}
}

func tarDir(ctx context.Context, archWriter *tar.Writer, path, currentPrefix string, decide decider) error {
	// The current entry is a directory that needs to be added to the archive.
	stat, err := os.Lstat(path)
	if err != nil {
		return err
	}

	link := ""
	if stat.Mode()&fs.ModeSymlink != 0 {
		link, err = os.Readlink(path)
		if err != nil {
			return fmt.Errorf("read link destination: %w", err)
		}
	}

	header, err := tar.FileInfoHeader(stat, link)
	if err != nil {
		return fmt.Errorf("file info header: %w", err)
	}
	header.Name = strings.TrimSuffix(currentPrefix, "/") + "/"
	header.Format = tarFormat

	if err := archWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("write file info header: %w", err)
	}

	if err := walkDir(ctx, archWriter, path, currentPrefix, decide); err != nil {
		return fmt.Errorf("walk dir: %w", err)
	}

	return nil
}

func tarFile(archWriter *tar.Writer, path, entryRelativePath string) error {
	entryPath := filepath.Join(path, filepath.Base(entryRelativePath))

	entryStat, err := os.Lstat(entryPath)
	if err != nil {
		return fmt.Errorf("file/link description: %w", err)
	}

	if entryStat.Mode()&fs.ModeSymlink != 0 {
		targetPath, err := os.Readlink(entryPath)
		if err != nil {
			return fmt.Errorf("read link destination: %w", err)
		}

		header := &tar.Header{
			Typeflag: tar.TypeSymlink,
			Name:     entryRelativePath,
			Linkname: targetPath,
			Format:   tarFormat,
			Mode:     int64(entryStat.Mode().Perm()),
		}

		if err := archWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("write symlink file info header: %w", err)
		}

		return nil
	}

	curFile, err := os.Open(entryPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func() { _ = curFile.Close() }()

	curFileStat, err := curFile.Stat()
	if err != nil {
		return fmt.Errorf("file description: %w", err)
	}

	header, err := tar.FileInfoHeader(curFileStat, curFileStat.Name())
	if err != nil {
		return fmt.Errorf("file info header: %w", err)
	}
	header.Name = entryRelativePath
	header.Format = tarFormat

	if err := archWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("write file info header: %w", err)
	}

	if _, err := io.Copy(archWriter, curFile); err != nil {
		return fmt.Errorf("copy file: %w", err)
	}

	return nil
}

func cancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
