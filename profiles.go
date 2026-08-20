package fastcdc

import (
	"fmt"
	"math/bits"
)

// Masks contains the three boundary masks for a chunking format.
type Masks struct {
	// Small is tested before AverageSize when normalization is enabled.
	Small uint64
	// Average is tested throughout when normalization is disabled.
	Average uint64
	// Large is tested from AverageSize onward when normalization is enabled.
	Large uint64
}

// maskTableDefault is the distributed mask table used by the v2020
// implementation in fastcdc-rs. Indexes are target-size bit counts; indexes 0
// through 4 are unused.
var maskTableDefault = [...]uint64{
	0, 0, 0, 0,
	0, 0x0000000001804110, 0x0000000001803110, 0x0000000018035100,
	0x0000001800035300, 0x0000019000353000, 0x0000590003530000, 0x0000d90003530000,
	0x0000d90103530000, 0x0000d90303530000, 0x0000d90313530000, 0x0000d90f03530000,
	0x0000d90303537000, 0x0000d90703537000, 0x0000d90707537000, 0x0000d91707537000,
	0x0000d91747537000, 0x0000d91767537000, 0x0000d93767537000, 0x0000d93777537000,
	0x0000d93777577000, 0x0000db3777577000,
}

// GearTableDefault returns the default FastCDC 2020 Gear table. It is used by
// fastcdc-go (Go), the v2020 implementation in nlfiedler/fastcdc-rs (Rust),
// and Fallen-Breath/pyfastcdc (Python). Leaving [Config.GearTable] zero selects
// these values.
//
// Entry i is the first big-endian uint64 of MD5(byte(i) repeated 64 times).
func GearTableDefault() [256]uint64 {
	return gearTableFastCDC2020
}

// GearTableC returns GEARv2 from wxiacode/FastCDC-c, the FastCDC authors' C
// implementation. Use [CConfig] to reproduce its scalar normalized 64-bit
// routine; selecting this table alone is not sufficient.
func GearTableC() [256]uint64 {
	return gearTableC
}

// MasksDefault returns the distributed masks used by fastcdc-go and the v2020
// implementation in fastcdc-rs for averageSize and normalization. The
// arguments follow the same rules as [Config.AverageSize] and
// [Config.Normalization], including zero selecting [NormalizationLevel1].
func MasksDefault(averageSize int, normalization Normalization) (Masks, error) {
	if err := validateAverageSize(averageSize); err != nil {
		return Masks{}, err
	}
	level, err := normalizationBits(normalization)
	if err != nil {
		return Masks{}, err
	}
	return deriveDefaultMasks(averageSize, level)
}

// MasksC returns the masks from wxiacode/FastCDC-c in small, average, and
// large chunk order. They are the fixed masks for its 8 KiB scalar normalized
// 64-bit routine. Use [CConfig] for the complete compatible configuration.
func MasksC() Masks {
	return Masks{
		Small:   0x0000d9f003530000,
		Average: 0x0000d93003530000,
		Large:   0x0000d90003530000,
	}
}

// CConfig returns the configuration of wxiacode/FastCDC-c's scalar
// normalized_chunking_64 routine: 6 KiB minimum, 8 KiB average, 32 KiB
// maximum, normalization level 2, GEARv2, and its three distributed masks.
func CConfig() Config {
	return Config{
		MinSize:       6 << 10,
		AverageSize:   8 << 10,
		MaxSize:       32 << 10,
		Normalization: NormalizationLevel2,
		Masks:         MasksC(),
		GearTable:     GearTableC(),
	}
}

func validateAverageSize(averageSize int) error {
	if averageSize < minAverageSize || averageSize > maxAverageSize {
		return fmt.Errorf("fastcdc: average size must be between 256 B and 4 MiB")
	}
	if averageSize&(averageSize-1) != 0 {
		return fmt.Errorf("fastcdc: average size must be a power of two")
	}
	return nil
}

func deriveDefaultMasks(averageSize, normalization int) (Masks, error) {
	averageBits := bits.Len(uint(averageSize)) - 1
	smallIndex := averageBits + normalization
	largeIndex := averageBits - normalization
	if smallIndex >= len(maskTableDefault) || largeIndex < 0 {
		return Masks{}, fmt.Errorf("fastcdc: normalization is unsupported for average size")
	}
	return Masks{
		Small:   maskTableDefault[smallIndex],
		Average: maskTableDefault[averageBits],
		Large:   maskTableDefault[largeIndex],
	}, nil
}
