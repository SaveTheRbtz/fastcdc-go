package main

import (
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

const (
	kiB = int64(1 << 10)
	miB = int64(1 << 20)
	giB = int64(1 << 30)
)

type chunkConfigFlags struct {
	average       string
	minimum       string
	maximum       string
	normalization string
}

type resolvedConfig struct {
	config        fastcdc.Config
	minimum       int
	average       int
	maximum       int
	normalization int
}

func addChunkConfigFlags(fs *flag.FlagSet) *chunkConfigFlags {
	values := &chunkConfigFlags{}
	fs.StringVar(&values.average, "average", "8KiB", "target average chunk size (power of two)")
	fs.StringVar(&values.minimum, "min", "", "minimum chunk size (default: average/4)")
	fs.StringVar(&values.maximum, "max", "", "maximum chunk size (default: average*4)")
	fs.StringVar(&values.normalization, "normalization", "1", "normalization: none, 1, 2, or 3")
	return values
}

func (values *chunkConfigFlags) resolve() (resolvedConfig, error) {
	average64, err := parseSize(values.average)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("average: %w", err)
	}
	minimum64 := average64 / 4
	if values.minimum != "" {
		minimum64, err = parseSize(values.minimum)
		if err != nil {
			return resolvedConfig{}, fmt.Errorf("min: %w", err)
		}
	}
	maximum64 := average64 * 4
	if values.maximum != "" {
		maximum64, err = parseSize(values.maximum)
		if err != nil {
			return resolvedConfig{}, fmt.Errorf("max: %w", err)
		}
	}
	if average64 > math.MaxInt || minimum64 > math.MaxInt || maximum64 > math.MaxInt {
		return resolvedConfig{}, fmt.Errorf("chunk size does not fit int on this platform")
	}

	normalization, normalizationBits, err := parseNormalization(values.normalization)
	if err != nil {
		return resolvedConfig{}, err
	}
	config := fastcdc.Config{
		AverageSize:   int(average64),
		MinSize:       int(minimum64),
		MaxSize:       int(maximum64),
		Normalization: normalization,
	}
	if _, err := fastcdc.New(config); err != nil {
		return resolvedConfig{}, err
	}
	return resolvedConfig{
		config:        config,
		minimum:       config.MinSize,
		average:       config.AverageSize,
		maximum:       config.MaxSize,
		normalization: normalizationBits,
	}, nil
}

func parseNormalization(value string) (fastcdc.Normalization, int, error) {
	switch strings.ToLower(value) {
	case "none", "0":
		return fastcdc.NormalizationNone, 0, nil
	case "1":
		return fastcdc.NormalizationLevel1, 1, nil
	case "2":
		return fastcdc.NormalizationLevel2, 2, nil
	case "3":
		return fastcdc.NormalizationLevel3, 3, nil
	default:
		return 0, 0, fmt.Errorf("normalization must be none, 1, 2, or 3")
	}
}

func parseSize(value string) (int64, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return 0, fmt.Errorf("size is empty")
	}

	multiplier := int64(1)
	lower := strings.ToLower(text)
	for _, suffix := range []struct {
		name       string
		multiplier int64
	}{
		{"gib", giB}, {"gb", 1_000_000_000},
		{"mib", miB}, {"mb", 1_000_000},
		{"kib", kiB}, {"kb", 1_000},
		{"b", 1},
	} {
		if strings.HasSuffix(lower, suffix.name) {
			multiplier = suffix.multiplier
			text = strings.TrimSpace(text[:len(text)-len(suffix.name)])
			break
		}
	}
	if text == "" {
		return 0, fmt.Errorf("size has no number")
	}
	number, err := strconv.ParseInt(text, 10, 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	if number != 0 && multiplier > math.MaxInt64/number {
		return 0, fmt.Errorf("size %q overflows int64", value)
	}
	return number * multiplier, nil
}

func formatBytes(value int64) string {
	switch {
	case value != 0 && value%giB == 0:
		return fmt.Sprintf("%dGiB", value/giB)
	case value != 0 && value%miB == 0:
		return fmt.Sprintf("%dMiB", value/miB)
	case value != 0 && value%kiB == 0:
		return fmt.Sprintf("%dKiB", value/kiB)
	default:
		return fmt.Sprintf("%dB", value)
	}
}

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }

func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}
