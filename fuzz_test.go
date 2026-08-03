package fastcdc

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"
)

func FuzzCutMatchesScalar(f *testing.F) {
	f.Add([]byte(nil), uint8(0), uint16(0), uint16(0), uint8(0))
	f.Add(make([]byte, 1024), uint8(0), uint16(64), uint16(1024), uint8(1))
	boundary := make([]byte, 1024)
	boundary[64] = 0xc0
	f.Add(boundary, uint8(0), uint16(65), uint16(1025), uint8(0))
	f.Add(splitMixBytes(2048, 2128), uint8(1), uint16(65), uint16(1025), uint8(3))
	f.Add(splitMixBytes(257, 588), uint8(1), uint16(65), uint16(1025), uint8(4))

	averages := [...]int{256, 512, 1024, 2048, 4096, 8192, 16384}
	normalizations := [...]Normalization{
		NormalizationNone,
		0,
		NormalizationLevel1,
		NormalizationLevel2,
		NormalizationLevel3,
	}
	f.Fuzz(func(t *testing.T, data []byte, averageIndex uint8, rawMin, rawMax uint16, normalizationIndex uint8) {
		average := averages[int(averageIndex)%len(averages)]
		minSize := 1 + int(rawMin)%max(1, average-1)
		maxSize := average + 1 + int(rawMax)%(4*average)
		chunker := mustChunker(t, Config{
			MinSize:       minSize,
			AverageSize:   average,
			MaxSize:       maxSize,
			Normalization: normalizations[int(normalizationIndex)%len(normalizations)],
		})

		if got, want := chunker.Cut(data), scalarCut(chunker, data); got != want {
			t.Fatalf("Cut = %d, scalarCut = %d (min=%d avg=%d max=%d len=%d)", got, want, minSize, average, maxSize, len(data))
		}

		offset := 0
		for chunkOffset, chunk := range chunker.Chunks(data) {
			if chunkOffset != offset || len(chunk) == 0 || cap(chunk) != len(chunk) {
				t.Fatalf("invalid chunk at offset %d: yielded offset=%d len=%d cap=%d", offset, chunkOffset, len(chunk), cap(chunk))
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
	f.Add(splitMixBytes(32768, 0x0123456789abcdef), []byte{1, 2, 3, 5, 8, 13}, uint8(4))

	normalizations := [...]Normalization{
		NormalizationNone,
		0,
		NormalizationLevel1,
		NormalizationLevel2,
		NormalizationLevel3,
	}
	f.Fuzz(func(t *testing.T, data, fragmentBytes []byte, normalizationIndex uint8) {
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
			Normalization: normalizations[int(normalizationIndex)%len(normalizations)],
		})
		want := collectMemoryChunks(chunker, data)
		got := collectReaderChunks(t, chunker.NewReader(&fragmentReader{data: data, sizes: sizes}))
		if !slices.EqualFunc(got, want, bytes.Equal) {
			t.Fatalf("stream chunks %v differ from slice chunks %v", chunkLengths(got), chunkLengths(want))
		}
	})
}

func FuzzReaderMatchesChunksWithErrors(f *testing.F) {
	f.Add([]byte(nil), []byte{0}, uint8(0))
	f.Add(make([]byte, 2048), []byte{0x80}, uint8(1))
	boundary := make([]byte, 1024)
	boundary[64] = 0xc0
	f.Add(boundary, []byte{0x60, 0x81}, uint8(0))
	f.Add(splitMixBytes(8192, 0x0123456789abcdef), []byte{0xe1, 0x02, 0xa3}, uint8(4))

	normalizations := [...]Normalization{
		NormalizationNone,
		0,
		NormalizationLevel1,
		NormalizationLevel2,
		NormalizationLevel3,
	}
	f.Fuzz(func(t *testing.T, data, directives []byte, normalizationIndex uint8) {
		if len(directives) == 0 {
			directives = []byte{0}
		}
		chunker := mustChunker(t, Config{
			MinSize:       65,
			AverageSize:   256,
			MaxSize:       1025,
			Normalization: normalizations[int(normalizationIndex)%len(normalizations)],
		})
		want := collectMemoryChunks(chunker, data)
		transient := errors.New("fuzz transient read error")
		source := &errorFragmentReader{
			data:       data,
			directives: directives,
			transient:  transient,
		}
		reader := chunker.NewReader(source)

		var got [][]byte
		var offset int64
		seenErrors := 0
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
			case transient:
				if chunk != nil {
					t.Fatalf("transient error returned with %d bytes", len(chunk))
				}
				if reader.InputOffset() != offset {
					t.Fatalf("InputOffset changed on error: got %d, want %d", reader.InputOffset(), offset)
				}
				seenErrors++
			case io.EOF:
				if chunk != nil {
					t.Fatalf("io.EOF returned with %d bytes", len(chunk))
				}
				if seenErrors != source.errors {
					t.Fatalf("observed %d transient errors, source returned %d", seenErrors, source.errors)
				}
				if !slices.EqualFunc(got, want, bytes.Equal) {
					t.Fatalf("stream chunks %v differ from slice chunks %v", chunkLengths(got), chunkLengths(want))
				}
				return
			default:
				t.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

// errorFragmentReader turns each directive into one productive read, preceded
// by optional (0, nil) and (0, transient) reads. A high bit adds the same
// transient error to the productive read. The low five bits select an 8- to
// 256-byte read size. The final productive read returns its bytes with io.EOF.
type errorFragmentReader struct {
	data          []byte
	directives    []byte
	transient     error
	offset        int
	index         int
	emptyReturned bool
	errorReturned bool
	errors        int
}

func (r *errorFragmentReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	directive := r.directives[r.index%len(r.directives)]
	if directive&0x20 != 0 && !r.emptyReturned {
		r.emptyReturned = true
		return 0, nil
	}
	if directive&0x40 != 0 && !r.errorReturned {
		r.errorReturned = true
		r.errors++
		return 0, r.transient
	}

	size := (int(directive&0x1f) + 1) * 8
	n := min(size, len(p), len(r.data)-r.offset)
	copy(p, r.data[r.offset:r.offset+n])
	r.offset += n
	r.index++
	r.emptyReturned = false
	r.errorReturned = false
	if r.offset == len(r.data) {
		return n, io.EOF
	}
	if directive&0x80 != 0 {
		r.errors++
		return n, r.transient
	}
	return n, nil
}
