package fastcdc

import (
	"bytes"
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
