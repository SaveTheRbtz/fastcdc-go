package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"flag"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

func TestChunkConfigFlagsResolve(t *testing.T) {
	t.Parallel()
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
	if resolved.minimum != 128 || resolved.average != 1024 || resolved.maximum != 4096 || resolved.normalization != 0 {
		t.Fatalf("resolved config = %+v", resolved)
	}
}

func TestParseSize(t *testing.T) {
	t.Parallel()
	tests := map[string]int64{
		"0": 0, "17B": 17, "2KiB": 2 << 10, "3MiB": 3 << 20,
		"1GiB": 1 << 30, "2KB": 2_000, " 4 mib ": 4 << 20,
	}
	for input, expected := range tests {
		input, expected := input, expected
		t.Run(input, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
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

func TestBenchmarkRoundAccountsForAllInput(t *testing.T) {
	t.Parallel()
	chunker, err := fastcdc.New(fastcdc.Config{AverageSize: 256, MinSize: 64, MaxSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	corpus := make([]byte, 4093)
	_, _ = io.ReadFull(newSplitMixReader(19), corpus)
	first, err := benchmarkRound(chunker, corpus, 1<<20+17)
	if err != nil {
		t.Fatal(err)
	}
	second, err := benchmarkRound(chunker, corpus, 1<<20+17)
	if err != nil {
		t.Fatal(err)
	}
	if first.bytes != 1<<20+17 || first.chunks == 0 {
		t.Fatalf("benchmark result = %+v", first)
	}
	if first.bytes != second.bytes || first.chunks != second.chunks || first.checksum != second.checksum {
		t.Fatalf("benchmark is not deterministic:\n%+v\n%+v", first, second)
	}
}

func TestRunBenchWritesCSV(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	if err := runBench([]string{
		"-bytes", "64KiB", "-corpus", "4KiB", "-rounds", "2",
	}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(&output).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || records[0][0] != "record" || records[1][0] != "round" || records[3][0] != "median" {
		t.Fatalf("unexpected benchmark CSV: %#v", records)
	}
}

func TestFileBenchmarkVerifiesReconstructionBeforeTiming(t *testing.T) {
	t.Parallel()
	content := make([]byte, 1<<20+31)
	_, _ = io.ReadFull(newSplitMixReader(91), content)
	path := filepath.Join(t.TempDir(), "input.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	chunker, err := fastcdc.New(fastcdc.Config{AverageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := verifyFileReconstruction(chunker, path, int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if digest != sha256.Sum256(content) {
		t.Fatalf("verified digest = %x, want %x", digest, sha256.Sum256(content))
	}
	first, err := benchmarkFileRound(chunker, path, int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := benchmarkFileRound(chunker, path, int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if first.bytes != int64(len(content)) || first.chunks == 0 || first.checksum != second.checksum {
		t.Fatalf("file benchmark results:\n%+v\n%+v", first, second)
	}
}

func TestAnalyticalDistributionSumsToOne(t *testing.T) {
	t.Parallel()
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

func TestDistributionRequiresACompleteChunk(t *testing.T) {
	t.Parallel()
	if err := runDistribution([]string{"-bytes", "1B"}, io.Discard, io.Discard); err == nil {
		t.Fatal("short distribution input unexpectedly succeeded")
	}
}

func TestTargetPartitionOracleFindsBoundaryCrossingChunk(t *testing.T) {
	t.Parallel()
	source := []byte("xxabcdefghyy")
	sourceChunks := [][]byte{source[:5], source[5:]}
	target := []analyzedChunk{{data: []byte("abcdefgh")}}
	actual, oracle := targetPartitionOracle(source, sourceChunks, target)
	if actual != 0 || oracle != 8 {
		t.Fatalf("actual, oracle = %d, %d; want 0, 8", actual, oracle)
	}
}

func TestDedupAnalyzerStratifiesAdjacentSnapshot(t *testing.T) {
	t.Parallel()
	chunker, err := fastcdc.New(fastcdc.Config{AverageSize: 256, MinSize: 64, MaxSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	unchanged := make([]byte, 4096)
	changedBefore := make([]byte, 4096)
	_, _ = io.ReadFull(newSplitMixReader(7), unchanged)
	_, _ = io.ReadFull(newSplitMixReader(8), changedBefore)
	changedAfter := bytes.Clone(changedBefore)
	changedAfter[len(changedAfter)/2] ^= 0xff
	source := memorySnapshot("first", map[string][]byte{
		"unchanged": unchanged,
		"changed":   changedBefore,
	})
	second := memorySnapshot("second", map[string][]byte{
		"unchanged": bytes.Clone(unchanged),
		"changed":   changedAfter,
		"new":       []byte("new"),
	})
	oracleStore, err := os.CreateTemp(t.TempDir(), "oracle-*.spool")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oracleStore.Close() })
	analyzer := &dedupAnalyzer{
		chunker:       chunker,
		globalChunks:  make(map[chunkKey]int),
		globalFiles:   make(map[fileKey]int),
		oracleEnabled: true,
		oracleMaxFile: 1 << 20,
		oracleStore:   oracleStore,
	}
	if _, err := analyzer.analyzeSnapshot(0, source); err != nil {
		t.Fatal(err)
	}
	metric, err := analyzer.analyzeSnapshot(1, second)
	if err != nil {
		t.Fatal(err)
	}
	if metric.earlierFileReuseBytes != int64(len(unchanged)) {
		t.Fatalf("earlier whole-file reuse = %d, want %d", metric.earlierFileReuseBytes, len(unchanged))
	}
	if metric.unchangedSamePathFiles != 1 || metric.unchangedSamePathBytes != int64(len(unchanged)) {
		t.Fatalf("unchanged files/bytes = %d/%d", metric.unchangedSamePathFiles, metric.unchangedSamePathBytes)
	}
	if metric.changedSamePathFiles != 1 || metric.changedSamePathBytes != int64(len(changedAfter)) {
		t.Fatalf("changed files/bytes = %d/%d", metric.changedSamePathFiles, metric.changedSamePathBytes)
	}
	if metric.newPathFiles != 1 || metric.newPathBytes != 3 {
		t.Fatalf("new files/bytes = %d/%d", metric.newPathFiles, metric.newPathBytes)
	}
	if metric.oracleEligibleChangedBytes != int64(len(changedAfter)) {
		t.Fatalf("oracle eligible bytes = %d, want %d", metric.oracleEligibleChangedBytes, len(changedAfter))
	}
	if metric.oracleReusableBytes < metric.oracleActualCDCReuseBytes {
		t.Fatalf("oracle reuse %d < exact boundary reuse %d", metric.oracleReusableBytes, metric.oracleActualCDCReuseBytes)
	}
	if metric.oracleActualCDCReuseBytes != metric.changedPathCDCReuse {
		t.Fatalf("eligible exact reuse %d != all-changed reuse %d when no files are skipped",
			metric.oracleActualCDCReuseBytes, metric.changedPathCDCReuse)
	}
	if metric.changedPathCDCReuse == 0 || metric.samePathCDCReuse < int64(len(unchanged)) {
		t.Fatalf("changed/all same-path CDC reuse = %d/%d", metric.changedPathCDCReuse, metric.samePathCDCReuse)
	}
}

func TestDirectorySnapshotUsesRelativeSlashPathsAndRegularFiles(t *testing.T) {
	t.Parallel()
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
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("directory files = %#v, want %#v", got, want)
	}
}

func TestDirectorySnapshotRejectsSymlinkRoot(t *testing.T) {
	t.Parallel()
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

func memorySnapshot(label string, files map[string][]byte) snapshot {
	return snapshot{label: label, eachFile: func(visit func(string, io.Reader, int64) error) error {
		paths := make([]string, 0, len(files))
		for path := range files {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			content := files[path]
			if err := visit(path, bytes.NewReader(content), int64(len(content))); err != nil {
				return err
			}
		}
		return nil
	}}
}

func TestGitRevisionSnapshotReadsRegularBlobs(t *testing.T) {
	t.Parallel()
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
	if !reflect.DeepEqual(got, want) {
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
