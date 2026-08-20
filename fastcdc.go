package fastcdc

import (
	"fmt"
	"io"
	"iter"
)

//go:generate go run ./internal/gentable

const (
	minAverageSize = 256
	maxAverageSize = 4 << 20
	maxChunkSize   = 16 << 20
)

// Normalization controls how tightly chunk sizes cluster around
// Config.AverageSize when [Config.Masks] is zero. With custom masks, it selects
// only between disabled and enabled normalization.
type Normalization int

const (
	// NormalizationNone disables chunk-size normalization.
	NormalizationNone Normalization = -1
	// NormalizationLevel1 is the default and, with derived masks, produces a
	// moderately narrow distribution of chunk sizes.
	NormalizationLevel1 Normalization = 1
	// NormalizationLevel2 derives a narrower distribution than level 1.
	NormalizationLevel2 Normalization = 2
	// NormalizationLevel3 derives the narrowest distribution.
	NormalizationLevel3 Normalization = 3
)

// Config defines a FastCDC chunking format. Use identical field values wherever
// identical chunk boundaries are required.
type Config struct {
	// AverageSize selects the target chunk scale, in bytes. It is required and
	// must be a power of two from 256 B through 4 MiB. It does not guarantee the
	// arithmetic mean of produced chunk sizes.
	AverageSize int

	// MinSize is the smallest content-defined chunk size, in bytes. The final
	// chunk may be shorter. Zero defaults to AverageSize/4; any other value must
	// be positive and less than AverageSize.
	MinSize int

	// MaxSize is the hard upper limit for a chunk, in bytes. Zero defaults to
	// AverageSize*4; any other value must exceed AverageSize and must not exceed
	// 16 MiB.
	MaxSize int

	// Normalization selects one of NormalizationNone or NormalizationLevel1
	// through NormalizationLevel3. Zero selects NormalizationLevel1.
	Normalization Normalization

	// Masks selects the boundary masks. The zero value derives the masks
	// returned by MasksDefault from AverageSize and Normalization. With custom
	// masks, NormalizationNone uses Average; enabled normalization uses Small
	// before AverageSize and Large afterward.
	Masks Masks

	// GearTable selects the lookup values used by the Gear rolling hash. The
	// zero value selects the table returned by GearTableDefault.
	GearTable [256]uint64
}

// Chunker defines a validated, immutable chunking format. Its methods are safe
// for concurrent use. The zero value is not usable; use [New].
type Chunker struct {
	minSize     int
	averageSize int
	maxSize     int
	maskSmall   uint64
	maskLarge   uint64
	gearTable   [256]uint64
}

// New returns a Chunker for config. It returns an error if config violates any
// of the documented size or normalization rules.
func New(config Config) (*Chunker, error) {
	if err := validateAverageSize(config.AverageSize); err != nil {
		return nil, err
	}

	minSize := config.MinSize
	if minSize == 0 {
		minSize = config.AverageSize / 4
	}
	maxSize := config.MaxSize
	if maxSize == 0 {
		maxSize = config.AverageSize * 4
	}
	if minSize <= 0 {
		return nil, fmt.Errorf("fastcdc: minimum size must be positive")
	}
	if maxSize > maxChunkSize {
		return nil, fmt.Errorf("fastcdc: maximum size must not exceed 16 MiB")
	}
	if minSize >= config.AverageSize || config.AverageSize >= maxSize {
		return nil, fmt.Errorf("fastcdc: sizes must satisfy MinSize < AverageSize < MaxSize")
	}

	normalization, err := normalizationBits(config.Normalization)
	if err != nil {
		return nil, err
	}
	maskSet := config.Masks
	if maskSet == (Masks{}) {
		maskSet, err = deriveDefaultMasks(config.AverageSize, normalization)
		if err != nil {
			return nil, err
		}
	}
	maskSmall, maskLarge := maskSet.Small, maskSet.Large
	if normalization == 0 {
		maskSmall, maskLarge = maskSet.Average, maskSet.Average
	}
	gearTable := config.GearTable
	if gearTable == ([256]uint64{}) {
		gearTable = GearTableDefault()
	}

	return &Chunker{
		minSize:     minSize,
		averageSize: config.AverageSize,
		maxSize:     maxSize,
		maskSmall:   maskSmall,
		maskLarge:   maskLarge,
		gearTable:   gearTable,
	}, nil
}

func normalizationBits(normalization Normalization) (int, error) {
	switch normalization {
	case 0, NormalizationLevel1:
		return 1, nil
	case NormalizationNone:
		return 0, nil
	case NormalizationLevel2:
		return 2, nil
	case NormalizationLevel3:
		return 3, nil
	default:
		return 0, fmt.Errorf("fastcdc: normalization must be None or Level1 through Level3")
	}
}

// Cut returns the length n of the first chunk, data[:n]. Data must begin at a
// chunk boundary and is treated as complete. If data ends before a boundary,
// Cut returns len(data) as the final chunk. Use [Chunker.NewReader] when more
// bytes may follow.
//
// For non-empty data, n is positive and does not exceed len(data) or the
// configured maximum chunk size. Cut returns zero only for empty data.
func (c *Chunker) Cut(data []byte) int {
	end := min(len(data), c.maxSize)
	if end <= c.minSize {
		return end
	}

	state := scanState{position: c.minSize}
	if cut := c.scan(data, end, &state); cut >= 0 {
		return cut
	}
	return end
}

// Chunks returns an iterator over the byte offset and contents of each chunk in
// complete data. The iterator may be ranged over repeatedly; each pass reads
// data again, so data must not be mutated while a pass is in progress.
//
// Each chunk aliases data and has its capacity clipped to its length.
func (c *Chunker) Chunks(data []byte) iter.Seq2[int, []byte] {
	return func(yield func(int, []byte) bool) {
		for offset := 0; offset < len(data); {
			length := c.Cut(data[offset:])
			end := offset + length
			if !yield(offset, data[offset:end:end]) {
				return
			}
			offset = end
		}
	}
}

// NewReader returns an independent Reader for src. It allocates a chunk buffer
// equal to the effective Config.MaxSize; [Reader.Reset] reuses that buffer.
// NewReader panics if src is nil.
func (c *Chunker) NewReader(src io.Reader) *Reader {
	if src == nil {
		panic("fastcdc: nil io.Reader")
	}
	r := &Reader{
		chunker: c,
		buffer:  make([]byte, c.maxSize),
	}
	r.Reset(src)
	return r
}

// scanState stores the scalar Gear hash so scanning can resume at any read
// boundary.
type scanState struct {
	position int
	hash     uint64
}

func (s *scanState) reset(minSize int) {
	s.position = minSize
	s.hash = 0
}

// scan examines every newly available candidate in data[:end]. It returns the
// first cut point, or -1 when more input is needed. state.position is relative
// to the beginning of the current chunk.
func (c *Chunker) scan(data []byte, end int, state *scanState) int {
	center := min(c.averageSize, end)
	if state.position < center {
		start := state.position
		cut, hash := c.scanPhase(data[start:center], state.hash, c.maskSmall)
		state.hash = hash
		if cut >= 0 {
			return start + cut
		}
		state.position = center
	}
	if state.position < end {
		start := state.position
		cut, hash := c.scanPhase(data[start:end], state.hash, c.maskLarge)
		state.hash = hash
		if cut >= 0 {
			return start + cut
		}
		state.position = end
	}
	return -1
}

// scanPhase preloads seven independent Gear values before applying the ordered
// hash steps. The short tail leaves hash resumable across input fragments.
func (c *Chunker) scanPhase(data []byte, hash, mask uint64) (int, uint64) {
	gear := &c.gearTable
	i := 0
	for ; i < len(data)-6; i += 7 {
		first := gear[data[i]]
		second := gear[data[i+1]]
		third := gear[data[i+2]]
		fourth := gear[data[i+3]]
		fifth := gear[data[i+4]]
		sixth := gear[data[i+5]]
		seventh := gear[data[i+6]]

		hash = (hash << 1) + first
		if hash&mask == 0 {
			return i, hash
		}
		hash = (hash << 1) + second
		if hash&mask == 0 {
			return i + 1, hash
		}
		hash = (hash << 1) + third
		if hash&mask == 0 {
			return i + 2, hash
		}
		hash = (hash << 1) + fourth
		if hash&mask == 0 {
			return i + 3, hash
		}
		hash = (hash << 1) + fifth
		if hash&mask == 0 {
			return i + 4, hash
		}
		hash = (hash << 1) + sixth
		if hash&mask == 0 {
			return i + 5, hash
		}
		hash = (hash << 1) + seventh
		if hash&mask == 0 {
			return i + 6, hash
		}
	}
	for ; i < len(data); i++ {
		hash = (hash << 1) + gear[data[i]]
		if hash&mask == 0 {
			return i, hash
		}
	}
	return -1, hash
}
