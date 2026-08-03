package main

import (
	"crypto/sha256"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

type chunkKey struct {
	digest [sha256.Size]byte
	size   uint32
}

type fileKey struct {
	digest [sha256.Size]byte
	size   int64
}

type priorFile struct {
	chunks        map[chunkKey]struct{}
	whole         fileKey
	oracleOffset  int64
	oracleSize    int
	boundaries    []chunkSpan
	oracleContent bool
}

type chunkSpan struct {
	start  int
	length int
}

type analyzedChunk struct {
	data []byte
}

type dedupMetrics struct {
	label string
	index int

	files        int64
	logicalBytes int64
	chunks       int64

	earlierChunkReuseBytes int64
	newUniqueChunkBytes    int64
	earlierFileReuseBytes  int64
	newUniqueFileBytes     int64
	samePathCDCReuse       int64
	changedPathCDCReuse    int64

	unchangedSamePathFiles int64
	unchangedSamePathBytes int64
	changedSamePathFiles   int64
	changedSamePathBytes   int64
	newPathFiles           int64
	newPathBytes           int64

	oracleEligibleChangedBytes int64
	oracleActualCDCReuseBytes  int64
	oracleReusableBytes        int64
	oracleSkippedFiles         int64
	oracleSkippedBytes         int64
}

type dedupAnalyzer struct {
	chunker *fastcdc.Chunker

	globalChunks map[chunkKey]int
	globalFiles  map[fileKey]int
	previous     map[string]priorFile
	havePrevious bool

	oracleEnabled bool
	oracleMaxFile int64
	oracleStore   *os.File

	totalLogical          int64
	totalFiles            int64
	totalChunks           int64
	totalUniqueChunkBytes int64
	totalUniqueFileBytes  int64
}

func runDedup(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("dedup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	chunkFlags := addChunkConfigFlags(fs)
	repository := fs.String("repo", "", "Git repository used by every -rev snapshot")
	var revisions stringList
	fs.Var(&revisions, "rev", "Git revision to analyze; repeat in comparison order")
	oracle := fs.Bool("oracle", false, "compute the exact adjacent-snapshot target-partition oracle")
	oracleMaxFileText := fs.String("oracle-max-file", "1MiB", "largest source and target file included in the oracle")
	if err := fs.Parse(args); err != nil {
		return err
	}
	resolved, err := chunkFlags.resolve()
	if err != nil {
		return err
	}
	if len(revisions) > 0 && *repository == "" {
		return fmt.Errorf("repo is required when rev is used")
	}
	if len(revisions) == 0 && *repository != "" {
		return fmt.Errorf("at least one rev is required when repo is used")
	}
	snapshotCount := fs.NArg() + len(revisions)
	if snapshotCount == 0 {
		return fmt.Errorf("provide at least one directory or -rev")
	}
	if *oracle && snapshotCount != 2 {
		return fmt.Errorf("oracle requires exactly two snapshots")
	}

	oracleMaxFile, err := parseSize(*oracleMaxFileText)
	if err != nil {
		return fmt.Errorf("oracle-max-file: %w", err)
	}
	if *oracle && (oracleMaxFile < 1 || strconv.IntSize == 32 && oracleMaxFile > 1<<31-1) {
		return fmt.Errorf("oracle-max-file must be positive and fit int on this platform")
	}

	chunker, err := fastcdc.New(resolved.config)
	if err != nil {
		return err
	}
	analyzer := &dedupAnalyzer{
		chunker:       chunker,
		globalChunks:  make(map[chunkKey]int),
		globalFiles:   make(map[fileKey]int),
		oracleEnabled: *oracle,
		oracleMaxFile: oracleMaxFile,
	}
	if analyzer.oracleEnabled {
		store, err := os.CreateTemp("", "fastcdc-lab-oracle-*.spool")
		if err != nil {
			return fmt.Errorf("create oracle spool: %w", err)
		}
		analyzer.oracleStore = store
		defer func() {
			_ = store.Close()
			_ = os.Remove(store.Name())
		}()
	}
	excludedFile := ""
	if analyzer.oracleStore != nil {
		excludedFile = analyzer.oracleStore.Name()
	}
	snapshots := make([]snapshot, 0, snapshotCount)
	for _, path := range fs.Args() {
		snapshots = append(snapshots, directorySnapshot(path, excludedFile))
	}
	for _, revision := range revisions {
		snapshots = append(snapshots, gitRevisionSnapshot(*repository, revision))
	}

	metrics := make([]dedupMetrics, 0, len(snapshots))
	for index, snapshot := range snapshots {
		metric, err := analyzer.analyzeSnapshot(index, snapshot)
		if err != nil {
			return err
		}
		metrics = append(metrics, metric)
	}
	if err := writeDedupCSV(stdout, analyzer, resolved, metrics); err != nil {
		return fmt.Errorf("write dedup report: %w", err)
	}
	return nil
}

func (a *dedupAnalyzer) analyzeSnapshot(index int, source snapshot) (dedupMetrics, error) {
	metric := dedupMetrics{label: source.label, index: index}
	current := make(map[string]priorFile)
	err := source.eachFile(func(path string, reader io.Reader, size int64) error {
		content, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("read %q in %s: %w", path, source.label, err)
		}
		if size >= 0 && int64(len(content)) != size {
			return fmt.Errorf("read %q in %s: got %d bytes, expected %d", path, source.label, len(content), size)
		}
		return a.analyzeFile(index, path, content, current, &metric)
	})
	if err != nil {
		return dedupMetrics{}, err
	}
	a.previous = current
	a.havePrevious = true
	a.totalFiles += metric.files
	a.totalLogical += metric.logicalBytes
	a.totalChunks += metric.chunks
	return metric, nil
}

func (a *dedupAnalyzer) analyzeFile(index int, path string, content []byte, current map[string]priorFile, metric *dedupMetrics) error {
	metric.files++
	metric.logicalBytes += int64(len(content))
	wholeKey := fileKey{digest: sha256.Sum256(content), size: int64(len(content))}
	if first, exists := a.globalFiles[wholeKey]; exists {
		if first < index {
			metric.earlierFileReuseBytes += int64(len(content))
		}
	} else {
		a.globalFiles[wholeKey] = index
		metric.newUniqueFileBytes += int64(len(content))
		a.totalUniqueFileBytes += int64(len(content))
	}

	prior, hasPriorPath := a.previous[path]
	isChangedSamePath := false
	if a.havePrevious {
		switch {
		case !hasPriorPath:
			metric.newPathFiles++
			metric.newPathBytes += int64(len(content))
		case prior.whole == wholeKey:
			metric.unchangedSamePathFiles++
			metric.unchangedSamePathBytes += int64(len(content))
		default:
			metric.changedSamePathFiles++
			metric.changedSamePathBytes += int64(len(content))
			isChangedSamePath = true
		}
	}
	chunks := make(map[chunkKey]struct{})
	var fileChunks []analyzedChunk
	if a.oracleEnabled {
		fileChunks = make([]analyzedChunk, 0, len(content)/8192+1)
	}
	for _, chunk := range a.chunker.Chunks(content) {
		key := chunkKey{digest: sha256.Sum256(chunk), size: uint32(len(chunk))}
		if a.oracleEnabled {
			fileChunks = append(fileChunks, analyzedChunk{data: chunk})
		}
		chunks[key] = struct{}{}
		metric.chunks++
		if first, exists := a.globalChunks[key]; exists {
			if first < index {
				metric.earlierChunkReuseBytes += int64(len(chunk))
			}
		} else {
			a.globalChunks[key] = index
			metric.newUniqueChunkBytes += int64(len(chunk))
			a.totalUniqueChunkBytes += int64(len(chunk))
		}
		if hasPriorPath {
			if _, exists := prior.chunks[key]; exists {
				metric.samePathCDCReuse += int64(len(chunk))
				if isChangedSamePath {
					metric.changedPathCDCReuse += int64(len(chunk))
				}
			}
		}
	}

	stored := priorFile{chunks: chunks, whole: wholeKey}
	if a.oracleEnabled && index == 0 && int64(len(content)) <= a.oracleMaxFile {
		offset, err := a.oracleStore.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("seek oracle spool: %w", err)
		}
		written, err := a.oracleStore.Write(content)
		if err != nil {
			return fmt.Errorf("write oracle spool: %w", err)
		}
		if written != len(content) {
			return fmt.Errorf("write oracle spool: %w", io.ErrShortWrite)
		}
		stored.oracleOffset = offset
		stored.oracleSize = len(content)
		stored.oracleContent = true
		stored.boundaries = make([]chunkSpan, len(fileChunks))
		start := 0
		for i := range fileChunks {
			stored.boundaries[i] = chunkSpan{start: start, length: len(fileChunks[i].data)}
			start += len(fileChunks[i].data)
		}
	}
	current[path] = stored

	if !a.oracleEnabled || !isChangedSamePath {
		return nil
	}
	if int64(len(content)) > a.oracleMaxFile || !prior.oracleContent {
		metric.oracleSkippedFiles++
		metric.oracleSkippedBytes += int64(len(content))
		return nil
	}
	metric.oracleEligibleChangedBytes += int64(len(content))
	priorContent := make([]byte, prior.oracleSize)
	if _, err := a.oracleStore.ReadAt(priorContent, prior.oracleOffset); err != nil {
		return fmt.Errorf("read oracle spool: %w", err)
	}
	priorChunks := make([][]byte, len(prior.boundaries))
	for i, span := range prior.boundaries {
		priorChunks[i] = priorContent[span.start : span.start+span.length]
	}
	actual, oracle := targetPartitionOracle(priorContent, priorChunks, fileChunks)
	metric.oracleActualCDCReuseBytes += actual
	metric.oracleReusableBytes += oracle
	if metric.oracleReusableBytes < metric.oracleActualCDCReuseBytes {
		return fmt.Errorf("internal error: target-partition oracle is below actual CDC reuse")
	}
	return nil
}

func writeDedupCSV(output io.Writer, analyzer *dedupAnalyzer, config resolvedConfig, metrics []dedupMetrics) error {
	w := csv.NewWriter(output)
	writeErr := w.Write([]string{
		"algorithm", "minimum_bytes", "average_bytes", "maximum_bytes", "normalization",
		"identity_digest", "identity_includes_size", "identity_byte_compared", "oracle_max_file_bytes",
		"snapshot_index", "snapshot", "files", "logical_bytes", "chunks",
		"earlier_snapshot_chunk_reuse_bytes", "new_unique_chunk_bytes",
		"earlier_snapshot_exact_file_reuse_bytes", "new_unique_exact_file_bytes",
		"unchanged_same_path_files", "unchanged_same_path_bytes",
		"changed_same_path_files", "changed_same_path_bytes", "new_path_files", "new_path_bytes",
		"adjacent_same_path_cdc_reuse_bytes", "changed_same_path_cdc_reuse_bytes",
		"oracle_eligible_changed_same_path_target_bytes",
		"oracle_actual_cdc_reuse_bytes", "target_partition_oracle_reuse_bytes",
		"source_boundary_loss_bytes", "oracle_skipped_files", "oracle_skipped_bytes",
		"all_snapshots_files", "all_snapshots_logical_bytes", "all_snapshots_chunks",
		"unique_chunk_bytes", "unique_exact_file_bytes",
	})
	for _, metric := range metrics {
		if writeErr != nil {
			break
		}
		oracleMaxFile := ""
		if analyzer.oracleEnabled {
			oracleMaxFile = strconv.FormatInt(analyzer.oracleMaxFile, 10)
		}
		writeErr = w.Write([]string{
			"FastCDC 2020", strconv.Itoa(config.minimum), strconv.Itoa(config.average),
			strconv.Itoa(config.maximum), strconv.Itoa(config.normalization),
			"SHA-256", "true", "false", oracleMaxFile,
			strconv.Itoa(metric.index), metric.label, strconv.FormatInt(metric.files, 10),
			strconv.FormatInt(metric.logicalBytes, 10), strconv.FormatInt(metric.chunks, 10),
			strconv.FormatInt(metric.earlierChunkReuseBytes, 10), strconv.FormatInt(metric.newUniqueChunkBytes, 10),
			strconv.FormatInt(metric.earlierFileReuseBytes, 10), strconv.FormatInt(metric.newUniqueFileBytes, 10),
			strconv.FormatInt(metric.unchangedSamePathFiles, 10), strconv.FormatInt(metric.unchangedSamePathBytes, 10),
			strconv.FormatInt(metric.changedSamePathFiles, 10), strconv.FormatInt(metric.changedSamePathBytes, 10),
			strconv.FormatInt(metric.newPathFiles, 10), strconv.FormatInt(metric.newPathBytes, 10),
			strconv.FormatInt(metric.samePathCDCReuse, 10), strconv.FormatInt(metric.changedPathCDCReuse, 10),
			strconv.FormatInt(metric.oracleEligibleChangedBytes, 10),
			strconv.FormatInt(metric.oracleActualCDCReuseBytes, 10), strconv.FormatInt(metric.oracleReusableBytes, 10),
			strconv.FormatInt(metric.oracleReusableBytes-metric.oracleActualCDCReuseBytes, 10),
			strconv.FormatInt(metric.oracleSkippedFiles, 10), strconv.FormatInt(metric.oracleSkippedBytes, 10),
			strconv.FormatInt(analyzer.totalFiles, 10), strconv.FormatInt(analyzer.totalLogical, 10),
			strconv.FormatInt(analyzer.totalChunks, 10), strconv.FormatInt(analyzer.totalUniqueChunkBytes, 10),
			strconv.FormatInt(analyzer.totalUniqueFileBytes, 10),
		})
	}
	w.Flush()
	if writeErr == nil {
		writeErr = w.Error()
	}
	return writeErr
}
