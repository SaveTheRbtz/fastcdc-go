package main

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

type benchResult struct {
	duration time.Duration
	bytes    int64
	chunks   int64
	digest   [sha256.Size]byte
}

func runBench(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	chunkFlags := addChunkConfigFlags(fs)
	inputSizeText := fs.String("bytes", "1GiB", "number of synthetic bytes per measured round")
	corpusSizeText := fs.String("corpus", "64MiB", "deterministic corpus repeated to make the input")
	filePath := fs.String("file", "", "read the entire regular file instead of synthetic input")
	verify := fs.Bool("verify", true, "verify a file's reconstructed SHA-256 before timing")
	rounds := fs.Int("rounds", 3, "number of measured rounds")
	seed := fs.Uint64("seed", 1, "SplitMix64 corpus seed (decimal or 0x-prefixed)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("bench takes no positional arguments")
	}
	if *rounds < 1 {
		return fmt.Errorf("rounds must be positive")
	}
	resolved, err := chunkFlags.resolve()
	if err != nil {
		return err
	}
	chunker, err := fastcdc.New(resolved.config)
	if err != nil {
		return err
	}

	var inputSize int64
	var measuredRound func() (benchResult, error)
	if *filePath != "" {
		forbidden := make(map[string]bool)
		fs.Visit(func(value *flag.Flag) { forbidden[value.Name] = true })
		for _, name := range []string{"bytes", "corpus", "seed"} {
			if forbidden[name] {
				return fmt.Errorf("%s cannot be used with file", name)
			}
		}
		info, err := os.Stat(*filePath)
		if err != nil {
			return fmt.Errorf("stat benchmark file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("benchmark file is not a regular file")
		}
		inputSize = info.Size()
		if inputSize <= 0 {
			return fmt.Errorf("benchmark file is empty")
		}
		if _, err := fmt.Fprintf(stdout, "input_file=%s bytes=%d rounds=%d average=%s min=%s max=%s normalization=%s\n",
			*filePath, inputSize, *rounds, formatBytes(int64(resolved.average)),
			formatBytes(int64(resolved.minimum)), formatBytes(int64(resolved.maximum)), chunkFlags.normalization); err != nil {
			return fmt.Errorf("write benchmark report: %w", err)
		}
		if *verify {
			digest, err := verifyFileReconstruction(chunker, *filePath, inputSize)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(stdout, "verified_bytes=%d verified_sha256=%x verification=untimed\n", inputSize, digest); err != nil {
				return fmt.Errorf("write benchmark report: %w", err)
			}
		}
		measuredRound = func() (benchResult, error) {
			return benchmarkFileRound(chunker, *filePath, inputSize)
		}
	} else {
		var err error
		inputSize, err = parseSize(*inputSizeText)
		if err != nil || inputSize <= 0 {
			return fmt.Errorf("bytes must be a positive size")
		}
		corpusSize, err := parseSize(*corpusSizeText)
		if err != nil || corpusSize <= 0 {
			return fmt.Errorf("corpus must be a positive size")
		}
		if corpusSize > int64(maxInt()) {
			return fmt.Errorf("corpus is too large for this platform")
		}
		corpus := make([]byte, int(corpusSize))
		if _, err := io.ReadFull(newSplitMixReader(*seed), corpus); err != nil {
			return fmt.Errorf("generate corpus: %w", err)
		}
		if _, err := fmt.Fprintf(stdout, "input=%s corpus=%s rounds=%d seed=%d average=%s min=%s max=%s normalization=%s\n",
			formatBytes(inputSize), formatBytes(corpusSize), *rounds, *seed,
			formatBytes(int64(resolved.average)), formatBytes(int64(resolved.minimum)),
			formatBytes(int64(resolved.maximum)), chunkFlags.normalization); err != nil {
			return fmt.Errorf("write benchmark report: %w", err)
		}
		measuredRound = func() (benchResult, error) {
			return benchmarkRound(chunker, corpus, inputSize)
		}
	}

	results := make([]benchResult, *rounds)
	for i := range results {
		result, err := measuredRound()
		if err != nil {
			return err
		}
		results[i] = result
		if _, err := fmt.Fprintf(stdout, "round=%d bytes=%d chunks=%d seconds=%.6f MiB/s=%.2f boundary_digest=%x\n",
			i+1, result.bytes, result.chunks, result.duration.Seconds(),
			float64(result.bytes)/float64(miB)/result.duration.Seconds(), result.digest); err != nil {
			return fmt.Errorf("write benchmark report: %w", err)
		}
	}
	for i := 1; i < len(results); i++ {
		if results[i].bytes != results[0].bytes || results[i].chunks != results[0].chunks || results[i].digest != results[0].digest {
			return fmt.Errorf("non-deterministic result between rounds 1 and %d", i+1)
		}
	}
	durations := make([]time.Duration, len(results))
	for i, result := range results {
		durations[i] = result.duration
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	median := durations[len(durations)/2]
	if len(durations)%2 == 0 {
		median = (durations[len(durations)/2-1] + median) / 2
	}
	if _, err := fmt.Fprintf(stdout, "median_seconds=%.6f median_MiB/s=%.2f\n",
		median.Seconds(), float64(inputSize)/float64(miB)/median.Seconds()); err != nil {
		return fmt.Errorf("write benchmark report: %w", err)
	}
	return nil
}

func benchmarkRound(chunker *fastcdc.Chunker, corpus []byte, inputSize int64) (benchResult, error) {
	source := &repeatingReader{pattern: corpus, remaining: inputSize}
	return benchmarkReader(chunker, source, inputSize)
}

func benchmarkFileRound(chunker *fastcdc.Chunker, path string, inputSize int64) (benchResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return benchResult{}, fmt.Errorf("open benchmark file: %w", err)
	}
	result, readErr := benchmarkReader(chunker, file, inputSize)
	closeErr := file.Close()
	if readErr != nil {
		return benchResult{}, readErr
	}
	if closeErr != nil {
		return benchResult{}, fmt.Errorf("close benchmark file: %w", closeErr)
	}
	return result, nil
}

func benchmarkReader(chunker *fastcdc.Chunker, source io.Reader, inputSize int64) (benchResult, error) {
	reader := chunker.NewReader(source)
	hash := sha256.New()
	var encodedLength [8]byte
	result := benchResult{}
	start := time.Now()
	for {
		chunk, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return benchResult{}, fmt.Errorf("chunk input: %w", err)
		}
		result.bytes += int64(len(chunk))
		result.chunks++
		binary.LittleEndian.PutUint64(encodedLength[:], uint64(len(chunk)))
		_, _ = hash.Write(encodedLength[:])
	}
	result.duration = time.Since(start)
	copy(result.digest[:], hash.Sum(nil))
	if result.bytes != inputSize {
		return benchResult{}, fmt.Errorf("processed %d bytes, want %d", result.bytes, inputSize)
	}
	return result, nil
}

func verifyFileReconstruction(chunker *fastcdc.Chunker, path string, expectedSize int64) ([sha256.Size]byte, error) {
	direct, directSize, err := hashFile(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if directSize != expectedSize {
		return [sha256.Size]byte{}, fmt.Errorf("benchmark file changed size: got %d, want %d", directSize, expectedSize)
	}
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("open benchmark file for chunk verification: %w", err)
	}
	hash := sha256.New()
	reader := chunker.NewReader(file)
	var reconstructed int64
	for {
		chunk, nextErr := reader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			_ = file.Close()
			return [sha256.Size]byte{}, fmt.Errorf("verify chunk reconstruction: %w", nextErr)
		}
		reconstructed += int64(len(chunk))
		_, _ = hash.Write(chunk)
	}
	if err := file.Close(); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("close benchmark verification file: %w", err)
	}
	var chunked [sha256.Size]byte
	copy(chunked[:], hash.Sum(nil))
	if reconstructed != expectedSize || chunked != direct {
		return [sha256.Size]byte{}, fmt.Errorf("chunk reconstruction mismatch: bytes=%d/%d sha256=%x/%x",
			reconstructed, expectedSize, chunked, direct)
	}
	return direct, nil
}

func hashFile(path string) ([sha256.Size]byte, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, 0, fmt.Errorf("open benchmark file for verification: %w", err)
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return [sha256.Size]byte{}, 0, fmt.Errorf("hash benchmark file: %w", copyErr)
	}
	if closeErr != nil {
		return [sha256.Size]byte{}, 0, fmt.Errorf("close benchmark file: %w", closeErr)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, size, nil
}

func maxInt() int { return int(^uint(0) >> 1) }
