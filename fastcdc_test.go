package fastcdc

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"
)

func TestNewDerivesConfig(t *testing.T) {
	type derivedConfig struct {
		minimum, average, maximum int
		maskSmall, maskLarge      uint64
	}
	minimumAverage := derivedConfig{
		minimum: 64, average: 256, maximum: 1024,
		maskSmall: masks[9], maskLarge: masks[7],
	}
	tests := []struct {
		name   string
		config Config
		want   derivedConfig
	}{
		{
			name:   "minimum average",
			config: Config{AverageSize: 256},
			want:   minimumAverage,
		},
		{
			name:   "explicit default normalization",
			config: Config{AverageSize: 256, Normalization: NormalizationLevel1},
			want:   minimumAverage,
		},
		{
			name:   "normalization disabled",
			config: Config{AverageSize: 8192, Normalization: NormalizationNone},
			want: derivedConfig{
				minimum: 2048, average: 8192, maximum: 32768,
				maskSmall: masks[13], maskLarge: masks[13],
			},
		},
		{
			name:   "normalization level 2",
			config: Config{AverageSize: 8192, Normalization: NormalizationLevel2},
			want: derivedConfig{
				minimum: 2048, average: 8192, maximum: 32768,
				maskSmall: masks[15], maskLarge: masks[11],
			},
		},
		{
			name:   "maximum average and normalization",
			config: Config{AverageSize: 4 << 20, Normalization: NormalizationLevel3},
			want: derivedConfig{
				minimum: 1 << 20, average: 4 << 20, maximum: 16 << 20,
				maskSmall: masks[25], maskLarge: masks[19],
			},
		},
		{
			name: "explicit odd bounds",
			config: Config{
				MinSize:       65,
				AverageSize:   512,
				MaxSize:       1025,
				Normalization: NormalizationNone,
			},
			want: derivedConfig{
				minimum: 65, average: 512, maximum: 1025,
				maskSmall: masks[9], maskLarge: masks[9],
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunker := mustChunker(t, test.config)
			got := derivedConfig{
				minimum: chunker.minSize, average: chunker.averageSize, maximum: chunker.maxSize,
				maskSmall: chunker.maskSmall, maskLarge: chunker.maskLarge,
			}
			if got != test.want {
				t.Errorf("New(%#v) = %#v, want %#v", test.config, got, test.want)
			}
		})
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{name: "missing average", config: Config{}},
		{name: "negative average", config: Config{AverageSize: -256}},
		{name: "average below minimum", config: Config{AverageSize: 128}},
		{name: "average above maximum", config: Config{AverageSize: (4 << 20) + 1}},
		{name: "average not power of two", config: Config{AverageSize: 768}},
		{name: "negative minimum", config: Config{MinSize: -1, AverageSize: 1024}},
		{name: "minimum equals average", config: Config{MinSize: 1024, AverageSize: 1024}},
		{name: "minimum above average", config: Config{MinSize: 2048, AverageSize: 1024}},
		{name: "maximum below average", config: Config{AverageSize: 1024, MaxSize: 512}},
		{name: "maximum equals average", config: Config{AverageSize: 1024, MaxSize: 1024}},
		{name: "maximum above limit", config: Config{AverageSize: 1024, MaxSize: (16 << 20) + 1}},
		{name: "normalization below range", config: Config{AverageSize: 1024, Normalization: -2}},
		{name: "normalization above range", config: Config{AverageSize: 1024, Normalization: 4}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunker, err := New(test.config)
			if err == nil {
				t.Fatalf("New(%#v) = %#v, nil; want an error", test.config, chunker)
			}
			if chunker != nil {
				t.Errorf("New returned a non-nil chunker on error: %#v", chunker)
			}
		})
	}
}

func TestCutSemantics(t *testing.T) {
	chunker := mustChunker(t, Config{AverageSize: 256})

	tests := []struct {
		name string
		data []byte
		want int
	}{
		{name: "nil", data: nil, want: 0},
		{name: "empty", data: []byte{}, want: 0},
		{name: "short tail", data: make([]byte, 63), want: 63},
		{name: "exact minimum", data: make([]byte, 64), want: 64},
		{name: "forced maximum", data: make([]byte, 2048), want: 1024},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := chunker.Cut(test.data); got != test.want {
				t.Errorf("Cut returned %d, want %d", got, test.want)
			}
		})
	}

	// The byte at a content cut influences the decision but belongs to the
	// next chunk. This catches the tempting but incompatible `return i+1`.
	withoutBoundary := make([]byte, 1024)
	boundary := slices.Clone(withoutBoundary)
	boundary[64] = 0xc0
	unnormalized := mustChunker(t, Config{
		AverageSize:   256,
		Normalization: NormalizationNone,
	})
	if got := unnormalized.Cut(withoutBoundary); got != 74 {
		t.Fatalf("all-zero Cut = %d, want 74", got)
	}
	if got := unnormalized.Cut(boundary); got != 64 {
		t.Fatalf("boundary Cut = %d, want 64", got)
	}
	if boundary[64] != 0xc0 {
		t.Fatal("test setup lost the boundary byte")
	}
}

func TestChunksSemantics(t *testing.T) {
	data := splitMixBytes(32768, 0x0123456789abcdef)
	chunker := mustChunker(t, Config{AverageSize: 1024})
	seq := chunker.Chunks(data)

	var firstLengths []int
	wantOffset := 0
	for offset, chunk := range seq {
		if offset != wantOffset {
			t.Fatalf("offset = %d, want %d", offset, wantOffset)
		}
		if len(chunk) == 0 {
			t.Fatal("Chunks yielded an empty chunk")
		}
		if cap(chunk) != len(chunk) {
			t.Errorf("chunk at %d has capacity %d, want clipped capacity %d", offset, cap(chunk), len(chunk))
		}
		if !slices.Equal(chunk, data[offset:offset+len(chunk)]) {
			t.Fatalf("chunk at %d does not equal the expected input region", offset)
		}
		if &chunk[0] != &data[offset] {
			t.Fatalf("chunk at %d does not alias the input", offset)
		}
		firstLengths = append(firstLengths, len(chunk))
		wantOffset += len(chunk)
	}
	if wantOffset != len(data) {
		t.Errorf("chunks end at %d, want %d", wantOffset, len(data))
	}
	// Chunks returns a reusable iterator rather than a single-use traversal.
	var secondLengths []int
	for _, chunk := range seq {
		secondLengths = append(secondLengths, len(chunk))
	}
	if !slices.Equal(secondLengths, firstLengths) {
		t.Errorf("reused iterator lengths = %v, want %v", secondLengths, firstLengths)
	}

	seen := 0
	for range seq {
		seen++
		break
	}
	if seen != 1 {
		t.Errorf("early break observed %d chunks, want 1", seen)
	}

	for range chunker.Chunks(nil) {
		t.Fatal("Chunks(nil) yielded a value")
	}

	// Confirm that the deciding byte is retained at the start of chunk two.
	boundary := make([]byte, 1024)
	boundary[64] = 0xc0
	unnormalized := mustChunker(t, Config{
		AverageSize:   256,
		Normalization: NormalizationNone,
	})
	index := 0
	for offset, chunk := range unnormalized.Chunks(boundary) {
		switch index {
		case 0:
			if offset != 0 || len(chunk) != 64 {
				t.Fatalf("first chunk = (%d, %d bytes), want (0, 64 bytes)", offset, len(chunk))
			}
		case 1:
			if offset != 64 || len(chunk) == 0 || chunk[0] != 0xc0 {
				t.Fatalf("second chunk does not start with deciding byte: offset=%d length=%d", offset, len(chunk))
			}
			return
		}
		index++
	}
	t.Fatal("boundary input yielded fewer than two chunks")
}

func TestInsertionDeletionLocality(t *testing.T) {
	chunker := mustChunker(t, Config{AverageSize: 8 << 10})
	original := splitMixBytes(1<<20, 0x0123456789abcdef)
	originalChunks := make(map[string]struct{})
	for _, chunk := range chunker.Chunks(original) {
		originalChunks[string(chunk)] = struct{}{}
	}

	editAt := len(original)/2 + 123
	insertedBytes := []byte("a small edit in the middle of a file")
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "insertion",
			data: slices.Concat(original[:editAt], insertedBytes, original[editAt:]),
		},
		{
			name: "deletion",
			data: slices.Concat(original[:editAt], original[editAt+len(insertedBytes):]),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reused := 0
			for _, chunk := range chunker.Chunks(test.data) {
				if _, ok := originalChunks[string(chunk)]; ok {
					reused += len(chunk)
				}
			}
			// A local edit should disturb only nearby chunks, not the remaining
			// half-megabyte suffix.
			if reused*10 < len(test.data)*9 {
				t.Fatalf("reused %d of %d bytes (%.2f%%), want at least 90%%",
					reused, len(test.data), 100*float64(reused)/float64(len(test.data)))
			}
		})
	}
}

func TestFastCDC2020Vectors(t *testing.T) {
	// These cut lengths were produced by fastcdc-rs at pinned commit
	// f76938d8c2d77799852415247c9b3e1fd91b73f3. The even-sized configuration
	// also matches its scalar v2016 implementation for every normalization.
	data := splitMixBytes(131072, 0x0123456789abcdef)
	digest := sha256.Sum256(data)
	if got := hex.EncodeToString(digest[:]); got != "68742a8e9d6b219abb62693d04df765a167adcf130fcd8b44acb83a8027c97ad" {
		t.Fatalf("generated corpus SHA-256 = %s", got)
	}

	level1 := []int{9932, 8608, 14648, 8810, 4576, 11519, 9690, 4732, 3589, 13882, 15829, 17049, 8208}
	tests := []struct {
		name          string
		normalization Normalization
		want          []int
	}{
		{
			name:          "none",
			normalization: NormalizationNone,
			want:          []int{9932, 23256, 8810, 4576, 25941, 3589, 13882, 4605, 28273, 4439, 3769},
		},
		{name: "default", normalization: 0, want: level1},
		{name: "level 1", normalization: NormalizationLevel1, want: level1},
		{
			name:          "level 2",
			normalization: NormalizationLevel2,
			want:          []int{8296, 9739, 10002, 9534, 5771, 3232, 9554, 11655, 8321, 12476, 17235, 13091, 3958, 8208},
		},
		{
			name:          "level 3",
			normalization: NormalizationLevel3,
			want:          []int{8296, 9739, 8757, 9108, 9810, 8842, 13231, 8312, 8931, 9565, 11224, 10297, 8852, 6108},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunker := mustChunker(t, Config{
				AverageSize:   8192,
				Normalization: test.normalization,
			})
			if got := cutLengths(chunker, data); !slices.Equal(got, test.want) {
				t.Errorf("chunk lengths:\n got %v\nwant %v", got, test.want)
			}
		})
	}
}

func TestPairedScanMatchesScalar(t *testing.T) {
	configs := []Config{
		{AverageSize: 256},
		{AverageSize: 256, Normalization: NormalizationNone},
		{AverageSize: 256, Normalization: NormalizationLevel2},
		{AverageSize: 256, Normalization: NormalizationLevel3},
		{MinSize: 65, AverageSize: 256, MaxSize: 1024, Normalization: NormalizationNone},
		{MinSize: 65, AverageSize: 512, MaxSize: 1025, Normalization: NormalizationNone},
		{MinSize: 257, AverageSize: 1024, MaxSize: 4093},
		{MinSize: 2049, AverageSize: 8192, MaxSize: 32767, Normalization: NormalizationLevel3},
	}

	for i, config := range configs {
		name := fmt.Sprintf("%d-%d-%d-norm%d", config.MinSize, config.AverageSize, config.MaxSize, config.Normalization)
		t.Run(name, func(t *testing.T) {
			chunker := mustChunker(t, config)
			data := splitMixBytes(chunker.maxSize*3+37, 0x9e3779b97f4a7c15+uint64(i))
			prefixes := []int{
				0,
				chunker.minSize - 1,
				chunker.minSize,
				chunker.minSize + 1,
				chunker.averageSize - 1,
				chunker.averageSize,
				chunker.averageSize + 1,
				chunker.maxSize - 1,
				chunker.maxSize,
				chunker.maxSize + 1,
			}
			for j, length := range prefixes {
				got := chunker.Cut(data[:length])
				want := scalarCut(chunker, data[:length])
				if got != want {
					t.Errorf("prefix %d (case %d): paired Cut = %d, scalar Cut = %d", length, j, got, want)
				}
			}

			for offset := 0; offset < len(data); {
				got := chunker.Cut(data[offset:])
				want := scalarCut(chunker, data[offset:])
				if got != want {
					t.Fatalf("offset %d: paired Cut = %d, scalar Cut = %d", offset, got, want)
				}
				if got <= 0 {
					t.Fatalf("offset %d: Cut returned non-positive length %d", offset, got)
				}
				offset += got
			}
		})
	}
}

func TestOddBoundRegressions(t *testing.T) {
	// The pinned Rust v2020 pair loop starts at min/2 and returns 64 here,
	// violating the odd minimum. The scalar algorithm starts at exactly 65.
	t.Run("odd minimum", func(t *testing.T) {
		data := make([]byte, 1024)
		data[64] = 0xc0
		chunker := mustChunker(t, Config{
			MinSize:       65,
			AverageSize:   256,
			MaxSize:       1024,
			Normalization: NormalizationNone,
		})
		assertScalarCut(t, chunker, data, 75)
	})

	// The pinned pair loop ignores the lone candidate at max-1 when MaxSize is
	// odd, forcing a 1025-byte chunk instead of the content cut at 1024.
	t.Run("odd maximum", func(t *testing.T) {
		data := splitMixBytes(2048, 2128)
		chunker := mustChunker(t, Config{
			MinSize:       64,
			AverageSize:   512,
			MaxSize:       1025,
			Normalization: NormalizationNone,
		})
		assertScalarCut(t, chunker, data, 1024)
	})

	// The same omitted-lone-candidate bug appears when an odd EOF tail is
	// shorter than AverageSize. The last candidate still must be examined.
	t.Run("odd final tail", func(t *testing.T) {
		data := splitMixBytes(257, 588)
		chunker := mustChunker(t, Config{
			MinSize:       64,
			AverageSize:   512,
			MaxSize:       1024,
			Normalization: NormalizationNone,
		})
		assertScalarCut(t, chunker, data, 256)
	})
}

func TestCanonicalTables(t *testing.T) {
	for i := range gear {
		if gearShifted[i] != gear[i]<<1 {
			t.Errorf("shifted Gear[%d] = %#016x, want %#016x", i, gearShifted[i], gear[i]<<1)
		}
	}

	digests := []struct {
		name  string
		table []uint64
		want  string
	}{
		{
			name:  "Gear",
			table: gear[:],
			want:  "9df0a720752a7d211fdebaf39bed01610983756fc340a1cfef41052b7356ae73",
		},
		{
			name:  "masks",
			table: masks[:],
			want:  "36cda7e03f84a23298e2f56d0a6a5c9064e8a9f71c4ed07c39b754496f8c720b",
		},
	}
	for _, test := range digests {
		t.Run(test.name, func(t *testing.T) {
			if got := uint64TableSHA256(test.table); got != test.want {
				t.Errorf("table SHA-256 = %s, want %s", got, test.want)
			}
		})
	}
}

func mustChunker(t testing.TB, config Config) *Chunker {
	t.Helper()
	chunker, err := New(config)
	if err != nil {
		t.Fatalf("New(%#v): %v", config, err)
	}
	return chunker
}

func cutLengths(chunker *Chunker, data []byte) []int {
	var lengths []int
	for _, chunk := range chunker.Chunks(data) {
		lengths = append(lengths, len(chunk))
	}
	return lengths
}

// scalarCut is the straightforward byte-at-a-time FastCDC scan. Keeping this
// oracle separate from scanPhase protects the paired optimization against
// alignment errors at odd minima, maxima, and fragment ends.
func scalarCut(chunker *Chunker, data []byte) int {
	end := min(len(data), chunker.maxSize)
	if end <= chunker.minSize {
		return end
	}

	center := min(chunker.averageSize, end)
	var hash uint64
	i := chunker.minSize
	for ; i < center; i++ {
		hash = (hash << 1) + gear[data[i]]
		if hash&chunker.maskSmall == 0 {
			return i
		}
	}
	for ; i < end; i++ {
		hash = (hash << 1) + gear[data[i]]
		if hash&chunker.maskLarge == 0 {
			return i
		}
	}
	return end
}

func assertScalarCut(t *testing.T, chunker *Chunker, data []byte, want int) {
	t.Helper()
	if got := scalarCut(chunker, data); got != want {
		t.Fatalf("test oracle returned %d, want %d", got, want)
	}
	if got := chunker.Cut(data); got != want {
		t.Errorf("paired Cut returned %d, want %d", got, want)
	}
}

func splitMixBytes(size int, seed uint64) []byte {
	data := make([]byte, 0, size+7)
	state := seed
	var word [8]byte
	for len(data) < size {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		binary.LittleEndian.PutUint64(word[:], z)
		data = append(data, word[:]...)
	}
	return data[:size]
}

func uint64TableSHA256(table []uint64) string {
	hash := sha256.New()
	var encoded [8]byte
	for _, value := range table {
		binary.BigEndian.PutUint64(encoded[:], value)
		_, _ = hash.Write(encoded[:])
	}
	return hex.EncodeToString(hash.Sum(nil))
}
