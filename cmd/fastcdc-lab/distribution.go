package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"strconv"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

type distributionBin struct {
	lower      int
	upper      int
	count      int64
	observed   float64
	analytical float64
}

func runDistribution(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("distribution", flag.ContinueOnError)
	fs.SetOutput(stderr)
	chunkFlags := addChunkConfigFlags(fs)
	inputSizeText := fs.String("bytes", "256MiB", "number of deterministic input bytes")
	binWidthText := fs.String("bin", "", "histogram bin width (default: average/16)")
	seed := fs.Uint64("seed", 1, "SplitMix64 input seed (decimal or 0x-prefixed)")
	csvPath := fs.String("csv", "fastcdc-distribution.csv", "output CSV path")
	svgPath := fs.String("svg", "fastcdc-distribution.svg", "output SVG path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("distribution takes no positional arguments")
	}
	inputSize, err := parseSize(*inputSizeText)
	if err != nil || inputSize <= 0 {
		return fmt.Errorf("bytes must be a positive size")
	}
	resolved, err := chunkFlags.resolve()
	if err != nil {
		return err
	}
	binWidth := int64(resolved.average / 16)
	if *binWidthText != "" {
		binWidth, err = parseSize(*binWidthText)
		if err != nil {
			return fmt.Errorf("bin: %w", err)
		}
	}
	if binWidth < 1 || binWidth > int64(resolved.maximum) {
		return fmt.Errorf("bin must be between 1B and max")
	}
	binCount := (int64(resolved.maximum-resolved.minimum) + binWidth) / binWidth
	if binCount > 4096 {
		return fmt.Errorf("bin produces %d rows; choose a width of at least %s", binCount,
			formatBytes((int64(resolved.maximum-resolved.minimum)+4095)/4096))
	}
	if *csvPath == "" || *svgPath == "" {
		return fmt.Errorf("csv and svg paths must not be empty")
	}

	chunker, err := fastcdc.New(resolved.config)
	if err != nil {
		return err
	}
	bins := makeDistributionBins(resolved, int(binWidth))
	completeChunks, completeBytes, finalTail, err := observeDistribution(chunker, inputSize, *seed, bins)
	if err != nil {
		return err
	}
	for i := range bins {
		if completeChunks != 0 {
			bins[i].observed = float64(bins[i].count) / float64(completeChunks)
		}
		bins[i].analytical = analyticalCDF(bins[i].upper, resolved) - analyticalCDF(bins[i].lower, resolved)
	}
	if err := writeDistributionCSV(*csvPath, resolved, bins); err != nil {
		return err
	}
	title := fmt.Sprintf("FastCDC distribution: avg %s, normalization %s",
		formatBytes(int64(resolved.average)), chunkFlags.normalization)
	if err := writeDistributionSVG(*svgPath, title, bins); err != nil {
		return err
	}
	observedMean := float64(completeBytes) / float64(max64(completeChunks, 1))
	if _, err := fmt.Fprintf(stdout,
		"input=%s seed=%d complete_chunks=%d complete_bytes=%d final_tail_bytes=%d\n"+
			"observed_mean=%.2f analytical_mean=%.2f model=independent-uniform-hash\n"+
			"csv=%s svg=%s\n",
		formatBytes(inputSize), *seed, completeChunks, completeBytes, finalTail,
		observedMean, analyticalMean(resolved), *csvPath, *svgPath); err != nil {
		return fmt.Errorf("write distribution report: %w", err)
	}
	return nil
}

func makeDistributionBins(config resolvedConfig, width int) []distributionBin {
	bins := make([]distributionBin, 0, (config.maximum-config.minimum+width-1)/width)
	for lower := config.minimum; lower <= config.maximum; lower += width {
		upper := lower + width
		if upper > config.maximum+1 {
			upper = config.maximum + 1
		}
		bins = append(bins, distributionBin{lower: lower, upper: upper})
	}
	return bins
}

func observeDistribution(chunker *fastcdc.Chunker, inputSize int64, seed uint64, bins []distributionBin) (int64, int64, int, error) {
	reader := chunker.NewReader(io.LimitReader(newSplitMixReader(seed), inputSize))
	var previous int
	var chunks, bytes int64
	for {
		chunk, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, 0, 0, fmt.Errorf("chunk synthetic input: %w", err)
		}
		if previous != 0 {
			addDistributionLength(bins, previous)
			chunks++
			bytes += int64(previous)
		}
		previous = len(chunk)
	}
	// A final MaxSize chunk is a forced full chunk, not an EOF-shortened tail.
	if previous == bins[len(bins)-1].upper-1 {
		addDistributionLength(bins, previous)
		chunks++
		bytes += int64(previous)
		previous = 0
	}
	return chunks, bytes, previous, nil
}

func addDistributionLength(bins []distributionBin, length int) {
	for i := range bins {
		if length >= bins[i].lower && length < bins[i].upper {
			bins[i].count++
			return
		}
	}
	panic(fmt.Sprintf("chunk length %d is outside histogram", length))
}

// analyticalCDF returns P(chunk length < length) under the usual model that
// each tested Gear hash is independently and uniformly distributed.
func analyticalCDF(length int, config resolvedConfig) float64 {
	if length <= config.minimum {
		return 0
	}
	if length > config.maximum {
		return 1
	}
	averageBits := bits.Len(uint(config.average)) - 1
	pSmall := math.Ldexp(1, -(averageBits + config.normalization))
	pLarge := math.Ldexp(1, -(averageBits - config.normalization))
	smallTrials := min(length, config.average) - config.minimum
	survival := math.Exp(float64(smallTrials) * math.Log1p(-pSmall))
	if length > config.average {
		largeTrials := length - config.average
		survival *= math.Exp(float64(largeTrials) * math.Log1p(-pLarge))
	}
	return 1 - survival
}

func analyticalMean(config resolvedConfig) float64 {
	averageBits := bits.Len(uint(config.average)) - 1
	pSmall := math.Ldexp(1, -(averageBits + config.normalization))
	pLarge := math.Ldexp(1, -(averageBits - config.normalization))
	qSmall := 1 - pSmall
	qLarge := 1 - pLarge
	smallTrials := config.average - config.minimum
	largeTrials := config.maximum - config.average
	smallSurvival := math.Pow(qSmall, float64(smallTrials))
	return float64(config.minimum) + geometricSum(qSmall, pSmall, smallTrials) +
		smallSurvival*geometricSum(qLarge, pLarge, largeTrials)
}

func geometricSum(q, oneMinusQ float64, count int) float64 {
	if count <= 0 {
		return 0
	}
	return q * (1 - math.Pow(q, float64(count))) / oneMinusQ
}

func writeDistributionCSV(path string, config resolvedConfig, bins []distributionBin) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create CSV: %w", err)
	}
	writer := csv.NewWriter(file)
	writeErr := writer.Write([]string{
		"minimum", "average", "maximum", "normalization",
		"lower_inclusive", "upper_exclusive", "observed_count",
		"observed_probability", "analytical_probability",
	})
	for _, bin := range bins {
		if writeErr != nil {
			break
		}
		writeErr = writer.Write([]string{
			strconv.Itoa(config.minimum), strconv.Itoa(config.average),
			strconv.Itoa(config.maximum), strconv.Itoa(config.normalization),
			strconv.Itoa(bin.lower), strconv.Itoa(bin.upper), strconv.FormatInt(bin.count, 10),
			strconv.FormatFloat(bin.observed, 'g', 12, 64),
			strconv.FormatFloat(bin.analytical, 'g', 12, 64),
		})
	}
	writer.Flush()
	if writeErr == nil {
		writeErr = writer.Error()
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return fmt.Errorf("write CSV: %w", writeErr)
	}
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
