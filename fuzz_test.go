package fastcdc

import (
	"bytes"
	"errors"
	"testing"
)

var fuzzNormalizations = [...]Normalization{
	NormalizationNone,
	0,
	NormalizationLevel1,
	NormalizationLevel2,
	NormalizationLevel3,
}

func FuzzCutMatchesScalar(f *testing.F) {
	f.Add([]byte(nil), uint8(0), uint16(0), uint16(0), uint8(0))
	// Average 256, minimum 64, maximum 1024, default normalization.
	f.Add(make([]byte, 1024), uint8(0), uint16(63), uint16(767), uint8(1))
	boundary := make([]byte, 1024)
	boundary[64] = 0xc0
	// Average 256, odd minimum 65, odd maximum 1025, no normalization.
	f.Add(boundary, uint8(0), uint16(64), uint16(768), uint8(0))
	// Average 512, odd minimum 65, odd maximum 1025.
	f.Add(splitMixBytes(2048, 2128), uint8(1), uint16(64), uint16(512), uint8(3))
	f.Add(splitMixBytes(257, 588), uint8(1), uint16(64), uint16(512), uint8(4))

	averages := [...]int{256, 512, 1024, 2048, 4096, 8192, 16384}
	f.Fuzz(func(t *testing.T, data []byte, averageIndex uint8, rawMin, rawMax uint16, normalizationIndex uint8) {
		if len(data) > 4<<10 {
			t.Skip()
		}
		average := averages[int(averageIndex)%len(averages)]
		minSize := 1 + int(rawMin)%max(1, average-1)
		maxSize := average + 1 + int(rawMax)%(4*average)
		chunker := mustChunker(t, Config{
			MinSize:       minSize,
			AverageSize:   average,
			MaxSize:       maxSize,
			Normalization: fuzzNormalizations[int(normalizationIndex)%len(fuzzNormalizations)],
		})

		if got, want := chunker.Cut(data), scalarCut(chunker, data); got != want {
			t.Fatalf("Cut = %d, scalarCut = %d (min=%d avg=%d max=%d len=%d)", got, want, minSize, average, maxSize, len(data))
		}

		offset := 0
		for chunkOffset, chunk := range chunker.Chunks(data) {
			if chunkOffset != offset || len(chunk) == 0 || cap(chunk) != len(chunk) {
				t.Fatalf("invalid chunk at offset %d: yielded offset=%d len=%d cap=%d", offset, chunkOffset, len(chunk), cap(chunk))
			}
			if want := scalarCut(chunker, data[offset:]); len(chunk) != want {
				t.Fatalf("chunk at offset %d has length %d, scalarCut = %d", offset, len(chunk), want)
			}
			offset += len(chunk)
		}
		if offset != len(data) {
			t.Fatalf("chunks cover %d bytes, want %d", offset, len(data))
		}
	})
}

func FuzzReaderMatchesChunks(f *testing.F) {
	f.Add([]byte(nil), []byte{1}, uint8(0))
	f.Add(make([]byte, 2048), []byte{1}, uint8(1))
	boundary := make([]byte, 1024)
	boundary[64] = 0xc0
	f.Add(boundary, []byte{64, 1}, uint8(0))
	f.Add(splitMixBytes(4096, 0x0123456789abcdef), []byte{1, 2, 3, 5, 8, 13}, uint8(4))

	f.Fuzz(func(t *testing.T, data, fragmentBytes []byte, normalizationIndex uint8) {
		if len(data) > 4<<10 || len(fragmentBytes) > 32 {
			t.Skip()
		}
		if len(fragmentBytes) == 0 {
			fragmentBytes = []byte{1}
		}
		sizes := make([]int, len(fragmentBytes))
		for i, size := range fragmentBytes {
			sizes[i] = int(size) + 1
		}
		chunker := mustChunker(t, Config{
			MinSize:       65,
			AverageSize:   256,
			MaxSize:       1025,
			Normalization: fuzzNormalizations[int(normalizationIndex)%len(fuzzNormalizations)],
		})
		want := collectMemoryChunks(chunker, data)
		got := collectReaderChunks(t, chunker.NewReader(&fragmentReader{data: data, sizes: sizes}))
		assertChunksEqual(t, got, want)
	})
}

func FuzzReaderTerminalErrors(f *testing.F) {
	f.Add([]byte(nil), []byte{1}, uint16(0), uint8(0))
	f.Add(make([]byte, 2048), []byte{1}, uint16(2048), uint8(1))
	boundary := make([]byte, 1024)
	boundary[64] = 0xc0
	f.Add(boundary, []byte{64, 1}, uint16(65), uint8(0))
	f.Add(splitMixBytes(4096, 0x0123456789abcdef), []byte{1, 2, 3, 5, 8, 13}, uint16(4096), uint8(4))

	f.Fuzz(func(t *testing.T, data, fragmentBytes []byte, rawPrefix uint16, normalizationIndex uint8) {
		if len(data) > 4<<10 || len(fragmentBytes) > 32 {
			t.Skip()
		}
		if len(fragmentBytes) == 0 {
			fragmentBytes = []byte{1}
		}
		sizes := make([]int, len(fragmentBytes))
		for i, size := range fragmentBytes {
			sizes[i] = int(size) + 1
		}
		chunker := mustChunker(t, Config{
			MinSize:       65,
			AverageSize:   256,
			MaxSize:       1025,
			Normalization: fuzzNormalizations[int(normalizationIndex)%len(fuzzNormalizations)],
		})
		prefixLen := int(rawPrefix) % (len(data) + 1)
		want := collectMemoryChunks(chunker, data[:prefixLen])
		terminal := errors.New("fuzz terminal read error")
		source := &terminalErrorReader{
			data:     data,
			sizes:    sizes,
			limit:    prefixLen,
			terminal: terminal,
		}
		reader := chunker.NewReader(source)

		var got [][]byte
		var offset int64
		for {
			if reader.InputOffset() != offset {
				t.Fatalf("InputOffset = %d, want %d", reader.InputOffset(), offset)
			}
			chunk, err := reader.Next()
			switch err {
			case nil:
				if len(chunk) == 0 || cap(chunk) != len(chunk) {
					t.Fatalf("invalid chunk: len=%d cap=%d", len(chunk), cap(chunk))
				}
				got = append(got, bytes.Clone(chunk))
				offset += int64(len(chunk))
			case terminal:
				if chunk != nil {
					t.Fatalf("terminal error returned with %d bytes", len(chunk))
				}
				if offset != int64(prefixLen) || reader.InputOffset() != int64(prefixLen) {
					t.Fatalf("InputOffset = %d after terminal error, want %d", reader.InputOffset(), prefixLen)
				}
				if source.offset != prefixLen {
					t.Fatalf("source accepted %d bytes, want %d", source.offset, prefixLen)
				}
				assertChunksEqual(t, got, want)

				calls := source.calls
				for range 2 {
					chunk, err = reader.Next()
					if chunk != nil || err != terminal {
						t.Fatalf("repeated Next = (%d-byte chunk, %v), want (nil, terminal error)", len(chunk), err)
					}
					if reader.InputOffset() != int64(prefixLen) {
						t.Fatalf("InputOffset after repeated error = %d, want %d", reader.InputOffset(), prefixLen)
					}
				}
				if source.calls != calls {
					t.Fatalf("repeated Next made %d additional source reads", source.calls-calls)
				}
				return
			default:
				t.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

// terminalErrorReader returns exactly limit bytes, split across reads, and
// attaches terminal to the last read. A zero limit returns the error alone.
type terminalErrorReader struct {
	data     []byte
	sizes    []int
	limit    int
	terminal error
	offset   int
	index    int
	calls    int
}

func (r *terminalErrorReader) Read(p []byte) (int, error) {
	r.calls++
	if r.offset == r.limit {
		return 0, r.terminal
	}
	size := r.sizes[r.index%len(r.sizes)]
	n := min(size, len(p), r.limit-r.offset)
	copy(p, r.data[r.offset:r.offset+n])
	r.offset += n
	r.index++
	if r.offset == r.limit {
		return n, r.terminal
	}
	return n, nil
}
