package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

func TestRunCSV(t *testing.T) {
	data := bytes.Repeat([]byte("content-defined storage\n"), 400)
	path := filepath.Join(t.TempDir(), "input.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	args := []string{
		"-file", path, "-avg", "256", "-min", "64", "-max", "1024",
		"-normalization", "2", "-csv",
	}
	if err := run(args, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("run() stderr = %q, want empty", stderr.String())
	}

	records, err := csv.NewReader(&stdout).ReadAll()
	if err != nil {
		t.Fatalf("parse run() CSV: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("run() CSV is empty")
	}
	if want := []string{"Offset", "Size"}; !slices.Equal(records[0], want) {
		t.Fatalf("run() CSV header = %v, want %v", records[0], want)
	}

	type boundary struct {
		offset int
		size   int
	}
	got := make([]boundary, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) != 2 {
			t.Fatalf("run() CSV record = %q, want two fields", record)
		}
		offset, err := strconv.Atoi(record[0])
		if err != nil {
			t.Fatalf("parse offset %q: %v", record[0], err)
		}
		size, err := strconv.Atoi(record[1])
		if err != nil {
			t.Fatalf("parse size %q: %v", record[1], err)
		}
		got = append(got, boundary{offset: offset, size: size})
	}

	chunker, err := fastcdc.New(fastcdc.Config{
		AverageSize: 256, MinSize: 64, MaxSize: 1024,
		Normalization: fastcdc.NormalizationLevel2,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := make([]boundary, 0, len(got))
	for offset, chunk := range chunker.Chunks(data) {
		want = append(want, boundary{offset: offset, size: len(chunk)})
	}
	if !slices.Equal(got, want) {
		t.Fatalf("run() boundaries = %v, want %v", got, want)
	}
}

func TestRunRejectsInvalidInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.bin")
	if err := os.WriteFile(path, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name          string
		args          []string
		want          string
		wantFlagError bool
	}{
		{name: "missing file flag", want: "flag -file is required"},
		{name: "unknown flag", args: []string{"-unknown"}, want: "flag provided but not defined", wantFlagError: true},
		{name: "missing input file", args: []string{"-file", filepath.Join(t.TempDir(), "missing")}, want: "open"},
		{name: "invalid chunk config", args: []string{"-file", path, "-avg", "257"}, want: "power of two"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			err := run(test.args, &bytes.Buffer{}, &stderr)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("run(%q) error = %v, want text %q", test.args, err, test.want)
			}
			if got := errors.Is(err, errInvalidFlags); got != test.wantFlagError {
				t.Errorf("errors.Is(run(%q), errInvalidFlags) = %t, want %t", test.args, got, test.wantFlagError)
			}
			if test.wantFlagError && strings.Count(stderr.String(), test.want) != 1 {
				t.Errorf("run(%q) stderr = %q, want one diagnostic containing %q", test.args, stderr.String(), test.want)
			}
		})
	}
}
