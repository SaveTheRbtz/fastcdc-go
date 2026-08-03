package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type snapshot struct {
	label    string
	eachFile func(func(path string, reader io.Reader, size int64) error) error
}

func directorySnapshot(root, excludedFile string) snapshot {
	return snapshot{
		label: "dir:" + root,
		eachFile: func(visit func(string, io.Reader, int64) error) error {
			rootPath, err := filepath.Abs(root)
			if err != nil {
				return fmt.Errorf("resolve directory %q: %w", root, err)
			}
			info, err := os.Lstat(rootPath)
			if err != nil {
				return fmt.Errorf("stat directory %q: %w", root, err)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("snapshot directory %q must not be a symlink", root)
			}
			if !info.IsDir() {
				return fmt.Errorf("snapshot %q is not a directory", root)
			}
			excludedPath := ""
			if excludedFile != "" {
				excludedPath, err = filepath.Abs(excludedFile)
				if err != nil {
					return fmt.Errorf("resolve excluded file: %w", err)
				}
			}
			return filepath.WalkDir(rootPath, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if path == excludedPath {
					return nil
				}
				if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
					return nil
				}
				entryInfo, err := entry.Info()
				if err != nil {
					return err
				}
				if !entryInfo.Mode().IsRegular() {
					return nil
				}
				relative, err := filepath.Rel(rootPath, path)
				if err != nil {
					return err
				}
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				visitErr := visit(filepath.ToSlash(relative), file, entryInfo.Size())
				closeErr := file.Close()
				if visitErr != nil {
					return visitErr
				}
				return closeErr
			})
		},
	}
}

type gitFile struct {
	path string
	oid  string
}

func gitRevisionSnapshot(repository, revision string) snapshot {
	return snapshot{
		label: fmt.Sprintf("git:%s:%s", repository, revision),
		eachFile: func(visit func(string, io.Reader, int64) error) error {
			files, err := listGitFiles(repository, revision)
			if err != nil {
				return err
			}
			return readGitFiles(repository, files, visit)
		},
	}
}

func listGitFiles(repository, revision string) ([]gitFile, error) {
	if strings.HasPrefix(revision, "-") {
		return nil, fmt.Errorf("revision must not begin with '-'")
	}
	verify := exec.Command("git", "-C", repository, "rev-parse", "--verify", "--end-of-options", revision+"^{tree}")
	verified, err := verify.Output()
	if err != nil {
		return nil, commandError("resolve Git revision", verify, err)
	}
	tree := strings.TrimSpace(string(verified))
	command := exec.Command("git", "-C", repository, "ls-tree", "-r", "-z", "--full-tree", tree)
	output, err := command.Output()
	if err != nil {
		return nil, commandError("list Git tree", command, err)
	}
	files := make([]gitFile, 0, bytes.Count(output, []byte{0}))
	for _, record := range bytes.Split(output, []byte{0}) {
		if len(record) == 0 {
			continue
		}
		tab := bytes.IndexByte(record, '\t')
		if tab < 0 {
			return nil, fmt.Errorf("parse git ls-tree record %q", record)
		}
		fields := strings.Fields(string(record[:tab]))
		if len(fields) != 3 {
			return nil, fmt.Errorf("parse git ls-tree metadata %q", record[:tab])
		}
		mode, objectType, oid := fields[0], fields[1], fields[2]
		if objectType != "blob" || !strings.HasPrefix(mode, "100") {
			continue
		}
		files = append(files, gitFile{path: string(record[tab+1:]), oid: oid})
	}
	return files, nil
}

func readGitFiles(repository string, files []gitFile, visit func(string, io.Reader, int64) error) error {
	command := exec.Command("git", "-C", repository, "cat-file", "--batch")
	input, err := command.StdinPipe()
	if err != nil {
		return fmt.Errorf("open git cat-file input: %w", err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open git cat-file output: %w", err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start git cat-file: %w", err)
	}
	w := bufio.NewWriter(input)
	r := bufio.NewReader(output)
	finish := func() error {
		closeErr := input.Close()
		waitErr := command.Wait()
		if waitErr != nil {
			return fmt.Errorf("git cat-file: %w: %s", waitErr, strings.TrimSpace(stderr.String()))
		}
		return closeErr
	}
	abort := func() {
		_ = input.Close()
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
	}
	for _, file := range files {
		if _, err := fmt.Fprintln(w, file.oid); err != nil {
			abort()
			return fmt.Errorf("query git object %s: %w", file.oid, err)
		}
		if err := w.Flush(); err != nil {
			abort()
			return fmt.Errorf("flush git object query: %w", err)
		}
		header, err := r.ReadString('\n')
		if err != nil {
			abort()
			return fmt.Errorf("read git object header for %q: %w", file.path, err)
		}
		fields := strings.Fields(header)
		if len(fields) != 3 || fields[1] != "blob" {
			abort()
			return fmt.Errorf("unexpected git cat-file header for %q: %q", file.path, strings.TrimSpace(header))
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 {
			abort()
			return fmt.Errorf("invalid git blob size for %q: %q", file.path, fields[2])
		}
		limited := &io.LimitedReader{R: r, N: size}
		if err := visit(file.path, limited, size); err != nil {
			abort()
			return err
		}
		if limited.N != 0 {
			abort()
			return fmt.Errorf("visitor left %d unread bytes in %q", limited.N, file.path)
		}
		terminator, err := r.ReadByte()
		if err != nil || terminator != '\n' {
			abort()
			return fmt.Errorf("invalid git cat-file terminator for %q", file.path)
		}
	}
	if err := finish(); err != nil {
		return err
	}
	return nil
}

func commandError(action string, command *exec.Cmd, err error) error {
	if exit, ok := err.(*exec.ExitError); ok {
		return fmt.Errorf("%s: %w: %s", action, err, strings.TrimSpace(string(exit.Stderr)))
	}
	return fmt.Errorf("%s: %w", action, err)
}
