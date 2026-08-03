package fastcdc

import (
	"fmt"
	"io"
	"iter"
	"math/bits"
)

const (
	minAverageSize = 256
	maxAverageSize = 4 << 20
	maxChunkSize   = 16 << 20
)

// Normalization controls how tightly chunk sizes cluster around
// Config.AverageSize.
type Normalization int

const (
	// NormalizationNone disables chunk-size normalization.
	NormalizationNone Normalization = -1
	// NormalizationLevel1 is the default and produces a moderately narrow
	// distribution of chunk sizes.
	NormalizationLevel1 Normalization = 1
	// NormalizationLevel2 produces a narrower distribution than level 1.
	NormalizationLevel2 Normalization = 2
	// NormalizationLevel3 produces the narrowest distribution.
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
}

// Chunker defines a validated, immutable chunking format. Its methods are safe
// for concurrent use. The zero value is not usable; use [New].
type Chunker struct {
	minSize     int
	averageSize int
	maxSize     int
	maskSmall   uint64
	maskLarge   uint64
}

// New returns a Chunker for config. It returns an error if config violates any
// of the documented size or normalization rules.
func New(config Config) (*Chunker, error) {
	if config.AverageSize < minAverageSize || config.AverageSize > maxAverageSize {
		return nil, fmt.Errorf("fastcdc: average size must be between 256 B and 4 MiB")
	}
	if config.AverageSize&(config.AverageSize-1) != 0 {
		return nil, fmt.Errorf("fastcdc: average size must be a power of two")
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
	averageBits := bits.Len(uint(config.AverageSize)) - 1
	smallIndex := averageBits + normalization
	largeIndex := averageBits - normalization
	if smallIndex >= len(masks) || largeIndex < 0 {
		return nil, fmt.Errorf("fastcdc: normalization is unsupported for average size")
	}

	return &Chunker{
		minSize:     minSize,
		averageSize: config.AverageSize,
		maxSize:     maxSize,
		maskSmall:   masks[smallIndex],
		maskLarge:   masks[largeIndex],
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

// NewReader returns an independent Reader for src. Creating a Reader allocates
// storage equal to the effective Config.MaxSize; [Reader.Reset] reuses it.
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

// scanState is always stored in the scalar Gear-hash representation. Pairing
// is local to scanPhase so the state can resume at any read boundary.
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
		cut, hash := scanPhase(data, state.position, center, state.hash, c.maskSmall)
		state.hash = hash
		if cut >= 0 {
			return cut
		}
		state.position = center
	}
	if state.position < end {
		cut, hash := scanPhase(data, state.position, end, state.hash, c.maskLarge)
		state.hash = hash
		if cut >= 0 {
			return cut
		}
		state.position = end
	}
	return -1
}

// scanPhase applies two exact scalar Gear steps per loop. The first temporary
// hash is shifted once, so it is tested with a shifted mask. An odd final
// candidate is processed scalarly, leaving hash resumable across fragments.
func scanPhase(data []byte, start, end int, hash, mask uint64) (int, uint64) {
	shiftedMask := mask << 1
	i := start
	for ; i+1 < end; i += 2 {
		hash = (hash << 2) + gearShifted[data[i]]
		if hash&shiftedMask == 0 {
			return i, hash
		}

		hash += gear[data[i+1]]
		if hash&mask == 0 {
			return i + 1, hash
		}
	}
	if i < end {
		hash = (hash << 1) + gear[data[i]]
		if hash&mask == 0 {
			return i, hash
		}
	}
	return -1, hash
}
