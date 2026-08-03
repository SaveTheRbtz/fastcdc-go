package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"io"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

func TestChunkConfigFlagsResolve(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	values := addChunkConfigFlags(fs)
	if err := fs.Parse([]string{
		"-average", "1KiB", "-min", "128B", "-max", "4KiB", "-normalization", "none",
	}); err != nil {
		t.Fatal(err)
	}
	resolved, err := values.resolve()
	if err != nil {
		t.Fatal(err)
	}
	want := resolvedConfig{
		config: fastcdc.Config{
			MinSize: 128, AverageSize: 1024, MaxSize: 4096,
			Normalization: fastcdc.NormalizationNone,
		},
		minimum: 128, average: 1024, maximum: 4096,
	}
	if resolved != want {
		t.Errorf("resolve() = %+v, want %+v", resolved, want)
	}
}

func TestParseSize(t *testing.T) {
	tests := map[string]int64{
		"0": 0, "17B": 17, "2KiB": 2 << 10, "3MiB": 3 << 20,
		"1GiB": 1 << 30, "2KB": 2_000, " 4 mib ": 4 << 20,
	}
	for input, expected := range tests {
		t.Run(input, func(t *testing.T) {
			actual, err := parseSize(input)
			if err != nil {
				t.Fatal(err)
			}
			if actual != expected {
				t.Fatalf("parseSize(%q) = %d, want %d", input, actual, expected)
			}
		})
	}
	for _, input := range []string{"", "B", "-1", "1.5MiB", "999999999999999999GiB"} {
		if _, err := parseSize(input); err == nil {
			t.Errorf("parseSize(%q) unexpectedly succeeded", input)
		}
	}
}

func TestSplitMixReaderIsIndependentOfReadSizes(t *testing.T) {
	const length = 1003
	oneRead := make([]byte, length)
	if _, err := io.ReadFull(newSplitMixReader(42), oneRead); err != nil {
		t.Fatal(err)
	}
	manyReads := make([]byte, length)
	reader := newSplitMixReader(42)
	for offset := 0; offset < len(manyReads); {
		width := min(offset%17+1, len(manyReads)-offset)
		if _, err := io.ReadFull(reader, manyReads[offset:offset+width]); err != nil {
			t.Fatal(err)
		}
		offset += width
	}
	if !bytes.Equal(oneRead, manyReads) {
		t.Fatal("SplitMix output changed with Read sizes")
	}
}

func TestAnalyticalDistributionSumsToOne(t *testing.T) {
	config := resolvedConfig{minimum: 64, average: 256, maximum: 1024, normalization: 1}
	bins := makeDistributionBins(config, 37)
	total := 0.0
	weighted := 0.0
	for _, bin := range bins {
		probability := analyticalCDF(bin.upper, config) - analyticalCDF(bin.lower, config)
		if probability < 0 {
			t.Fatalf("negative bin probability %g", probability)
		}
		total += probability
	}
	for length := config.minimum; length <= config.maximum; length++ {
		probability := analyticalCDF(length+1, config) - analyticalCDF(length, config)
		weighted += float64(length) * probability
	}
	if math.Abs(total-1) > 1e-12 {
		t.Fatalf("bin probabilities sum to %.16g, want 1", total)
	}
	if mean := analyticalMean(config); mean <= float64(config.minimum) || mean >= float64(config.maximum) {
		t.Fatalf("analytical mean %.2f lies outside chunk bounds", mean)
	} else if math.Abs(mean-weighted) > 1e-9 {
		t.Fatalf("analytical mean %.12g, PMF mean %.12g", mean, weighted)
	}
}

func TestRunDistribution(t *testing.T) {
	t.Run("writes a complete CSV report", func(t *testing.T) {
		var output bytes.Buffer
		if err := run([]string{
			"distribution",
			"-average", "256B",
			"-min", "64B",
			"-max", "1KiB",
			"-bytes", "64KiB",
			"-bin", "64B",
			"-seed", "7",
		}, &output, io.Discard); err != nil {
			t.Fatal(err)
		}

		rows := readCSVRows(t, &output)
		if len(rows) < 2 {
			t.Fatalf("distribution report has %d rows, want multiple bins", len(rows))
		}
		first := rows[0]
		if first["minimum_bytes"] != "64" || first["average_bytes"] != "256" ||
			first["maximum_bytes"] != "1024" || first["input_bytes"] != "65536" || first["seed"] != "7" {
			t.Fatalf("distribution metadata = %#v", first)
		}
		completeChunks := csvInt64(t, first, "complete_chunks")
		completeBytes := csvInt64(t, first, "complete_bytes")
		finalTail := csvInt64(t, first, "final_tail_bytes")
		if completeChunks <= 0 || completeBytes+finalTail != 64<<10 {
			t.Fatalf("complete chunks/bytes/tail = %d/%d/%d", completeChunks, completeBytes, finalTail)
		}
		var observedChunks int64
		for _, row := range rows {
			observedChunks += csvInt64(t, row, "observed_count")
		}
		if observedChunks != completeChunks {
			t.Fatalf("histogram contains %d chunks, report says %d", observedChunks, completeChunks)
		}
	})

	t.Run("rejects input without a complete chunk", func(t *testing.T) {
		if err := run([]string{"distribution", "-bytes", "1B"}, io.Discard, io.Discard); err == nil {
			t.Fatal("short distribution input unexpectedly succeeded")
		}
	})
}

func TestTargetPartitionOracleFindsBoundaryCrossingChunk(t *testing.T) {
	source := []byte("xxabcdefghyy")
	sourceChunks := [][]byte{source[:5], source[5:]}
	target := []analyzedChunk{{data: []byte("abcdefgh")}}
	actual, oracle := targetPartitionOracle(source, sourceChunks, target)
	if actual != 0 || oracle != 8 {
		t.Fatalf("actual, oracle = %d, %d; want 0, 8", actual, oracle)
	}
}

func TestRunDedupStratifiesAdjacentSnapshots(t *testing.T) {
	unchanged := make([]byte, 4096)
	changedBefore := make([]byte, 4096)
	_, _ = io.ReadFull(newSplitMixReader(7), unchanged)
	_, _ = io.ReadFull(newSplitMixReader(8), changedBefore)
	changedAfter := bytes.Clone(changedBefore)
	changedAfter[len(changedAfter)/2] ^= 0xff

	first := t.TempDir()
	second := t.TempDir()
	writeSnapshot := func(root string, files map[string][]byte) {
		t.Helper()
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(root, name), content, 0o644); err != nil {
				t.Fatalf("write %s: %v", name, err)
			}
		}
	}
	writeSnapshot(first, map[string][]byte{
		"unchanged": unchanged,
		"changed":   changedBefore,
	})
	writeSnapshot(second, map[string][]byte{
		"unchanged": bytes.Clone(unchanged),
		"changed":   changedAfter,
		"new":       []byte("new"),
	})

	var output bytes.Buffer
	if err := run([]string{
		"dedup",
		"-average", "256B",
		"-min", "64B",
		"-max", "1KiB",
		"-oracle",
		first,
		second,
	}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	rows := readCSVRows(t, &output)
	if len(rows) != 2 {
		t.Fatalf("dedup report has %d rows, want 2", len(rows))
	}
	metric := rows[1]
	if metric["snapshot_index"] != "1" || metric["files"] != "3" || metric["logical_bytes"] != "8195" {
		t.Fatalf("second snapshot metadata = %#v", metric)
	}
	if metric["earlier_snapshot_exact_file_reuse_bytes"] != "4096" ||
		metric["unchanged_same_path_files"] != "1" || metric["unchanged_same_path_bytes"] != "4096" {
		t.Fatalf("unchanged classification = %#v", metric)
	}
	if metric["changed_same_path_files"] != "1" || metric["changed_same_path_bytes"] != "4096" {
		t.Fatalf("changed classification = %#v", metric)
	}
	if metric["new_path_files"] != "1" || metric["new_path_bytes"] != "3" {
		t.Fatalf("new classification = %#v", metric)
	}
	if metric["oracle_eligible_changed_same_path_target_bytes"] != "4096" {
		t.Fatalf("oracle-eligible bytes = %q, want 4096", metric["oracle_eligible_changed_same_path_target_bytes"])
	}
	actual := csvInt64(t, metric, "oracle_actual_cdc_reuse_bytes")
	oracle := csvInt64(t, metric, "target_partition_oracle_reuse_bytes")
	if oracle < actual {
		t.Fatalf("target-partition oracle reuse %d < actual CDC reuse %d", oracle, actual)
	}
	changedReuse := csvInt64(t, metric, "changed_same_path_cdc_reuse_bytes")
	if actual != changedReuse || changedReuse == 0 {
		t.Fatalf("oracle actual/changed-path reuse = %d/%d", actual, changedReuse)
	}
}

func TestDirectorySnapshotUsesRelativeSlashPathsAndRegularFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "nested", "more"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "top"), []byte("top"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "more", "file"), []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}
	excluded := filepath.Join(root, "excluded")
	if err := os.WriteFile(excluded, []byte("excluded"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "top"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	got := make(map[string]string)
	if err := directorySnapshot(root, excluded).eachFile(func(path string, reader io.Reader, size int64) error {
		content, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		if int64(len(content)) != size {
			t.Fatalf("%s: size %d, read %d", path, size, len(content))
		}
		got[path] = string(content)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"top": "top", "nested/more/file": "nested"}
	if !maps.Equal(got, want) {
		t.Fatalf("directory files = %#v, want %#v", got, want)
	}
}

func TestDirectorySnapshotRejectsSymlinkRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}
	err := directorySnapshot(link, "").eachFile(func(string, io.Reader, int64) error { return nil })
	if err == nil {
		t.Fatal("symlink snapshot root unexpectedly succeeded")
	}
}

func TestGitRevisionSnapshotLabelIsRepositoryIndependent(t *testing.T) {
	const revision = "0123456789abcdef"
	got := gitRevisionSnapshot("/private/machine-specific/path", revision).label
	if want := "git:" + revision; got != want {
		t.Fatalf("snapshot label = %q, want %q", got, want)
	}
}

func TestGitRevisionSnapshotReadsRegularBlobs(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	if err := os.WriteFile(filepath.Join(repository, "file.txt"), []byte("revision contents"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(repository, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "sub", "empty"), nil, 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=FastCDC Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "test")

	got := make(map[string][]byte)
	snapshot := gitRevisionSnapshot(repository, "HEAD")
	if err := snapshot.eachFile(func(path string, reader io.Reader, size int64) error {
		content, err := io.ReadAll(reader)
		if err == nil && int64(len(content)) != size {
			t.Fatalf("%s: size %d, read %d", path, size, len(content))
		}
		got[path] = content
		return err
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string][]byte{"file.txt": []byte("revision contents"), "sub/empty": {}}
	if !maps.EqualFunc(got, want, bytes.Equal) {
		t.Fatalf("Git files = %#v, want %#v", got, want)
	}
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, arguments...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", arguments, err, output)
	}
}

func readCSVRows(t *testing.T, input io.Reader) []map[string]string {
	t.Helper()
	records, err := csv.NewReader(input).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV report: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("CSV report is empty")
	}

	header := records[0]
	columns := make(map[string]struct{}, len(header))
	for _, name := range header {
		if name == "" {
			t.Fatal("CSV report has an empty column name")
		}
		if _, exists := columns[name]; exists {
			t.Fatalf("CSV report repeats column %q", name)
		}
		columns[name] = struct{}{}
	}
	rows := make([]map[string]string, 0, len(records)-1)
	for _, record := range records[1:] {
		row := make(map[string]string, len(header))
		for i, name := range header {
			row[name] = record[i]
		}
		rows = append(rows, row)
	}
	return rows
}

func csvInt64(t *testing.T, row map[string]string, field string) int64 {
	t.Helper()
	value, ok := row[field]
	if !ok {
		t.Fatalf("CSV report lacks %q column", field)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		t.Fatalf("CSV field %q = %q: %v", field, value, err)
	}
	return parsed
}
