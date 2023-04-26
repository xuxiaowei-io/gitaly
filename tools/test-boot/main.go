package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

type gitalyConfig struct {
	SocketPath     string
	BinDir         string
	Dir            string
	UseBundledGit  bool
	GitPath        string
	GitalyDir      string
	GitlabShellDir string
}

const configTemplate = `
socket_path = "{{.SocketPath}}"
bin_dir = "{{.BinDir}}"

[[storage]]
name = "default"
path = "{{.Dir}}"

[git]
use_bundled_binaries = {{.UseBundledGit}}
bin_path = "{{.GitPath}}"

[gitlab-shell]
dir = "{{.GitlabShellDir}}"

[gitlab]
url = 'http://gitlab_url'
`

func parseArgs() (string, bool, error) {
	useBundledGit := flag.Bool("bundled-git", false, "Set up Gitaly with bundled Git binaries")
	notUseBundledGit := flag.Bool("no-bundled-git", false, "Set up Gitaly without bundled Git binaries")

	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		return "", false, fmt.Errorf("gitaly source directory not provided")
	}
	if flag.NArg() > 1 {
		usage()
		return "", false, fmt.Errorf("extra arguments")
	}

	if *useBundledGit && *notUseBundledGit {
		usage()
		return "", false, fmt.Errorf("use either --bundled-git or --no-bundled-git, not both")
	}

	gitalyDir, err := filepath.Abs(flag.Args()[0])
	if err != nil {
		return "", false, fmt.Errorf("failed to parse dir %v: %w", flag.Args()[0], err)
	}

	return gitalyDir, *useBundledGit, nil
}

func checkVersion(gitalyDir, gitalyBin string) error {
	versionCmd := exec.Command(gitalyBin, "-version")
	versionOutput, err := versionCmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get Gitaly version output: %w", err)
	}

	version := strings.TrimSpace(strings.TrimPrefix(string(versionOutput), "Gitaly, version "))

	versionFromFile, err := os.ReadFile(filepath.Join(gitalyDir, "VERSION"))
	if err != nil {
		return fmt.Errorf("failed to read Gitaly version file: %w", err)
	}
	// Use strings.HasPrefix() because the version output could use git describe, if it is a source install
	// e.g.: Gitaly, version 1.75.0-14-gd1ecb43f
	if !strings.HasPrefix(version, string(versionFromFile)) {
		return fmt.Errorf("version check failed: VERSION file contained %q\n"+ //nolint:stylecheck
			"but 'gitaly -version' reported %q.\n"+
			"If you are working from a fork, please fetch the latest tags.\n",
			versionFromFile, version)
	}

	return nil
}

func writeGitalyConfig(path string, params gitalyConfig) error {
	t, err := template.New("config").Parse(configTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create Gitaly config file: %w", err)
	}
	defer f.Close()

	if err := t.Execute(f, params); err != nil {
		return fmt.Errorf("generate Gitaly config file: %w", err)
	}

	return nil
}

func spawnAndWait(gitalyBin, configPath, socketPath string) (returnedError error) {
	cmd := exec.Command(gitalyBin, configPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start gitaly: %w", err)
	}

	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if returnedError != nil {
			fmt.Fprintf(os.Stdout, "%s\n", stdout.String())
			fmt.Fprintf(os.Stderr, "%s\n", stderr.String())
		}
	}()

	start := time.Now()
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("unix", socketPath)
		if err == nil {
			fmt.Printf("\n\nconnection established after %v\n\n", time.Since(start))
			conn.Close()
			return nil
		}

		fmt.Printf(".")
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("")

	return fmt.Errorf("failed to connect to gitaly after %v", time.Since(start))
}

func testBoot() error {
	gitalyDir, useBundledGit, err := parseArgs()
	if err != nil {
		return err
	}

	buildDir := filepath.Join(gitalyDir, "_build")
	binDir := filepath.Join(buildDir, "bin")

	gitPath := filepath.Join(buildDir, "deps", "git-distribution", "bin-wrappers", "git")
	if useBundledGit {
		gitPath = ""
	}

	gitalyBin := filepath.Join(binDir, "gitaly")
	if err := checkVersion(gitalyDir, gitalyBin); err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp("", "gitaly-test-boot")
	if err != nil {
		return fmt.Errorf("create temp directory: %w", err)
	}

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	if err := os.Mkdir(gitlabShellDir, 0o755); err != nil {
		return fmt.Errorf("create gitlab-shell directory: %w", err)
	}

	err = os.WriteFile(filepath.Join(gitlabShellDir, ".gitlab_shell_secret"), []byte("test_gitlab_shell_token"), 0o644)
	if err != nil {
		return fmt.Errorf("write gitlab-shell secret: %w", err)
	}

	socketPath := filepath.Join(tempDir, "socket")
	configPath := filepath.Join(tempDir, "config.toml")
	err = writeGitalyConfig(configPath,
		gitalyConfig{
			SocketPath:     socketPath,
			BinDir:         binDir,
			Dir:            tempDir,
			UseBundledGit:  useBundledGit,
			GitPath:        gitPath,
			GitalyDir:      gitalyDir,
			GitlabShellDir: gitlabShellDir,
		})
	if err != nil {
		return nil
	}

	if err := spawnAndWait(gitalyBin, configPath, socketPath); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := testBoot(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [options] <GITALY_DIR>\n", filepath.Base(os.Args[0]))
	fmt.Fprintf(os.Stderr, "  --[no-]bundled-git    Set up Gitaly with bundled Git binaries\n\n")
}