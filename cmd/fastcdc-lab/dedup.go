package main

import (
	"crypto/sha256"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"

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
	csvPath := fs.String("csv", "", "optional machine-readable summary CSV")
	oracle := fs.Bool("oracle", false, "compute the exact adjacent-snapshot target-partition oracle")
	oracleMaxFileText := fs.String("oracle-max-file", "1MiB", "largest source and target file included in the oracle")
	oracleTempDir := fs.String("oracle-temp-dir", "", "directory for the oracle content spool (default: system temp)")
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

	snapshots := make([]snapshot, 0, fs.NArg()+len(revisions))
	for _, path := range fs.Args() {
		snapshots = append(snapshots, directorySnapshot(path))
	}
	for _, revision := range revisions {
		snapshots = append(snapshots, gitRevisionSnapshot(*repository, revision))
	}
	if len(snapshots) == 0 {
		return fmt.Errorf("provide at least one directory or -rev")
	}

	oracleMaxFile, err := parseSize(*oracleMaxFileText)
	if err != nil {
		return fmt.Errorf("oracle-max-file: %w", err)
	}
	if *oracle && (oracleMaxFile < 1 || oracleMaxFile > int64(maxInt())) {
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
		store, err := os.CreateTemp(*oracleTempDir, "fastcdc-lab-oracle-*.spool")
		if err != nil {
			return fmt.Errorf("create oracle spool: %w", err)
		}
		analyzer.oracleStore = store
		defer func() {
			_ = store.Close()
			_ = os.Remove(store.Name())
		}()
	}

	metrics := make([]dedupMetrics, 0, len(snapshots))
	for index, snapshot := range snapshots {
		metric, err := analyzer.analyzeSnapshot(index, snapshot)
		if err != nil {
			return err
		}
		metrics = append(metrics, metric)
	}
	if err := writeDedupReport(stdout, analyzer, metrics, resolved); err != nil {
		return fmt.Errorf("write dedup report: %w", err)
	}
	if *csvPath != "" {
		if err := writeDedupCSV(*csvPath, analyzer, resolved, metrics); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(stdout, "csv=%s\n", *csvPath); err != nil {
			return fmt.Errorf("write dedup report: %w", err)
		}
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
	fileChunks := make([]analyzedChunk, 0, len(content)/8192+1)
	for _, chunk := range a.chunker.Chunks(content) {
		key := chunkKey{digest: sha256.Sum256(chunk), size: uint32(len(chunk))}
		fileChunks = append(fileChunks, analyzedChunk{data: chunk})
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
	if a.oracleEnabled && int64(len(content)) <= a.oracleMaxFile {
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

func writeDedupReport(output io.Writer, analyzer *dedupAnalyzer, metrics []dedupMetrics, config resolvedConfig) error {
	var report []byte
	report = fmt.Appendf(report, "algorithm\tFastCDC 2020; average=%s min=%s max=%s normalization=%d\n",
		formatBytes(int64(config.average)), formatBytes(int64(config.minimum)),
		formatBytes(int64(config.maximum)), config.normalization)
	report = append(report, "identity\tSHA-256 digest plus length (hash collisions are not byte-compared)\n"...)
	for _, metric := range metrics {
		report = fmt.Appendf(report, "snapshot\t%d\t%s\n", metric.index, metric.label)
		report = fmt.Appendf(report, "  files\t%d\n", metric.files)
		report = fmt.Appendf(report, "  logical_bytes\t%d\n", metric.logicalBytes)
		report = fmt.Appendf(report, "  chunks\t%d\n", metric.chunks)
		report = fmt.Appendf(report, "  earlier_snapshot_chunk_reuse_bytes\t%d\t%.2f%% of snapshot\n",
			metric.earlierChunkReuseBytes, percent(metric.earlierChunkReuseBytes, metric.logicalBytes))
		report = fmt.Appendf(report, "  newly_stored_unique_chunk_bytes\t%d\n", metric.newUniqueChunkBytes)
		report = fmt.Appendf(report, "  earlier_snapshot_exact_file_reuse_bytes\t%d\t%.2f%% of snapshot\n",
			metric.earlierFileReuseBytes, percent(metric.earlierFileReuseBytes, metric.logicalBytes))
		report = fmt.Appendf(report, "  newly_stored_unique_exact_file_bytes\t%d\n", metric.newUniqueFileBytes)
		if metric.index > 0 {
			report = fmt.Appendf(report, "  unchanged_same_path_files_bytes\t%d\t%d\n",
				metric.unchangedSamePathFiles, metric.unchangedSamePathBytes)
			report = fmt.Appendf(report, "  changed_same_path_files_bytes\t%d\t%d\n",
				metric.changedSamePathFiles, metric.changedSamePathBytes)
			report = fmt.Appendf(report, "  new_path_files_bytes\t%d\t%d\n",
				metric.newPathFiles, metric.newPathBytes)
			report = fmt.Appendf(report, "  adjacent_same_path_CDC_reuse_bytes\t%d\t%.2f%% of snapshot\n",
				metric.samePathCDCReuse, percent(metric.samePathCDCReuse, metric.logicalBytes))
			report = fmt.Appendf(report, "  changed_same_path_CDC_reuse_bytes\t%d\t%.2f%% of changed same-path bytes\n",
				metric.changedPathCDCReuse, percent(metric.changedPathCDCReuse, metric.changedSamePathBytes))
		}
		if analyzer.oracleEnabled && metric.index > 0 {
			report = fmt.Appendf(report, "  oracle_eligible_changed_same_path_target_bytes\t%d\n", metric.oracleEligibleChangedBytes)
			report = fmt.Appendf(report, "  oracle_actual_CDC_reuse_bytes\t%d\t%.2f%% of eligible target\n",
				metric.oracleActualCDCReuseBytes, percent(metric.oracleActualCDCReuseBytes, metric.oracleEligibleChangedBytes))
			report = fmt.Appendf(report, "  target_partition_oracle_reuse_bytes\t%d\t%.2f%% of eligible target\n",
				metric.oracleReusableBytes, percent(metric.oracleReusableBytes, metric.oracleEligibleChangedBytes))
			report = fmt.Appendf(report, "  source_boundary_loss_bytes\t%d\n",
				metric.oracleReusableBytes-metric.oracleActualCDCReuseBytes)
			report = fmt.Appendf(report, "  oracle_skipped_files_bytes\t%d\t%d\n",
				metric.oracleSkippedFiles, metric.oracleSkippedBytes)
		}
	}
	report = append(report, "all_snapshots\n"...)
	report = fmt.Appendf(report, "  files\t%d\n", analyzer.totalFiles)
	report = fmt.Appendf(report, "  logical_bytes\t%d\n", analyzer.totalLogical)
	report = fmt.Appendf(report, "  chunks\t%d\n", analyzer.totalChunks)
	report = fmt.Appendf(report, "  unique_chunk_bytes\t%d\t%.2f%% storage saving\n",
		analyzer.totalUniqueChunkBytes, saving(analyzer.totalUniqueChunkBytes, analyzer.totalLogical))
	report = fmt.Appendf(report, "  unique_exact_file_bytes\t%d\t%.2f%% storage saving\n",
		analyzer.totalUniqueFileBytes, saving(analyzer.totalUniqueFileBytes, analyzer.totalLogical))
	if analyzer.oracleEnabled {
		report = fmt.Appendf(report, "oracle_scope\tadjacent snapshots, changed same-path content, fixed target FastCDC partition; files > %s excluded\n",
			formatBytes(analyzer.oracleMaxFile))
		report = append(report, "oracle_note\tExact byte comparison via suffix array; this is not a global theoretical optimum.\n"...)
	}

	tabs := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	written, err := tabs.Write(report)
	if err != nil {
		return err
	}
	if written != len(report) {
		return io.ErrShortWrite
	}
	return tabs.Flush()
}

func writeDedupCSV(path string, analyzer *dedupAnalyzer, config resolvedConfig, metrics []dedupMetrics) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create dedup CSV: %w", err)
	}
	w := csv.NewWriter(file)
	writeErr := w.Write([]string{
		"minimum", "average", "maximum", "normalization", "oracle_max_file",
		"snapshot_index", "snapshot", "files", "logical_bytes", "chunks",
		"earlier_snapshot_chunk_reuse_bytes", "new_unique_chunk_bytes",
		"earlier_snapshot_exact_file_reuse_bytes", "new_unique_exact_file_bytes",
		"unchanged_same_path_files", "unchanged_same_path_bytes",
		"changed_same_path_files", "changed_same_path_bytes", "new_path_files", "new_path_bytes",
		"adjacent_same_path_cdc_reuse_bytes", "changed_same_path_cdc_reuse_bytes",
		"oracle_eligible_changed_same_path_target_bytes",
		"oracle_actual_cdc_reuse_bytes", "target_partition_oracle_reuse_bytes",
		"source_boundary_loss_bytes", "oracle_skipped_files", "oracle_skipped_bytes",
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
			strconv.Itoa(config.minimum), strconv.Itoa(config.average), strconv.Itoa(config.maximum),
			strconv.Itoa(config.normalization), oracleMaxFile,
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
		})
	}
	w.Flush()
	if writeErr == nil {
		writeErr = w.Error()
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return fmt.Errorf("write dedup CSV: %w", writeErr)
	}
	return nil
}

func percent(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return 100 * float64(part) / float64(whole)
}

func saving(stored, logical int64) float64 {
	if logical == 0 {
		return 0
	}
	return 100 * (1 - float64(stored)/float64(logical))
}
