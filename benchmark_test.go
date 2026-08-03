package fastcdc

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"testing"
)

const benchmarkDataSize = 64 << 20

func BenchmarkPairedVersusScalar(b *testing.B) {
	textPattern := []byte("The quick brown fox jumps over the lazy dog.\n")
	chunker := mustChunker(b, Config{AverageSize: 64 << 10})
	scanners := []struct {
		name string
		cut  func([]byte) int
	}{
		{name: "paired", cut: chunker.Cut},
		{name: "scalar", cut: func(data []byte) int { return scalarCut(chunker, data) }},
	}
	inputs := []struct {
		name  string
		build func() []byte
	}{
		{
			name: "random",
			build: func() []byte {
				return splitMixBytes(benchmarkDataSize, 0x0123456789abcdef)
			},
		},
		{
			name: "text",
			build: func() []byte {
				return bytes.Repeat(textPattern, benchmarkDataSize/len(textPattern)+1)[:benchmarkDataSize]
			},
		},
		{
			name: "zero",
			build: func() []byte {
				return make([]byte, benchmarkDataSize)
			},
		},
		{
			name: "mixed",
			build: func() []byte {
				data := splitMixBytes(benchmarkDataSize, 0xfedcba9876543210)
				for start := 1 << 20; start < len(data); start += 2 << 20 {
					clear(data[start:min(start+(1<<20), len(data))])
				}
				return data
			},
		},
	}

	for _, input := range inputs {
		data := input.build()
		for _, scanner := range scanners {
			b.Run(input.name+"/"+scanner.name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				b.ResetTimer()
				for range b.N {
					offset := 0
					for offset < len(data) {
						offset += scanner.cut(data[offset:])
					}
					if offset != len(data) {
						b.Fatalf("consumed %d bytes, want %d", offset, len(data))
					}
				}
			})
		}
	}
}

func BenchmarkChunks(b *testing.B) {
	data := splitMixBytes(benchmarkDataSize, 0x0123456789abcdef)
	for _, average := range []int{8 << 10, 64 << 10, 1 << 20} {
		chunker := mustChunker(b, Config{AverageSize: average})
		b.Run(strconv.Itoa(average), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				consumed := 0
				for _, chunk := range chunker.Chunks(data) {
					consumed += len(chunk)
				}
				if consumed != len(data) {
					b.Fatalf("consumed %d bytes, want %d", consumed, len(data))
				}
			}
		})
	}
}

func BenchmarkReader(b *testing.B) {
	data := splitMixBytes(benchmarkDataSize, 0xfedcba9876543210)
	for _, average := range []int{8 << 10, 64 << 10, 1 << 20} {
		chunker := mustChunker(b, Config{AverageSize: average})
		b.Run(strconv.Itoa(average), func(b *testing.B) {
			source := bytes.NewReader(data)
			reader := chunker.NewReader(source)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				source.Reset(data)
				reader.Reset(source)
				consumed, err := consumeReader(reader)
				if err != nil {
					b.Fatal(err)
				}
				if consumed != int64(len(data)) {
					b.Fatalf("consumed %d bytes, want %d", consumed, len(data))
				}
			}
		})
	}
}

// BenchmarkReaderFile is opt-in because it is intended for warm-cache runs on
// real files of at least 1 GiB. Set FASTCDC_BENCH_FILE to enable it.
func BenchmarkReaderFile(b *testing.B) {
	path := os.Getenv("FASTCDC_BENCH_FILE")
	if path == "" {
		b.Skip("set FASTCDC_BENCH_FILE to a file of at least 1 GiB")
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		b.Fatalf("%q is not a regular file", path)
	}
	if info.Size() < 1<<30 {
		b.Fatalf("%q is %d bytes; want at least 1 GiB", path, info.Size())
	}

	chunker := mustChunker(b, Config{AverageSize: 1 << 20})
	reader := chunker.NewReader(bytes.NewReader(nil))
	b.ReportAllocs()
	b.SetBytes(info.Size())
	b.ResetTimer()
	for range b.N {
		file, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		reader.Reset(file)
		consumed, readErr := consumeReader(reader)
		closeErr := file.Close()
		if readErr != nil {
			b.Fatal(readErr)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		if consumed != info.Size() {
			b.Fatalf("consumed %d bytes, want %d", consumed, info.Size())
		}
	}
}

func consumeReader(reader *Reader) (int64, error) {
	var consumed int64
	for {
		chunk, err := reader.Next()
		if err == io.EOF {
			return consumed, nil
		}
		if err != nil {
			return 0, err
		}
		consumed += int64(len(chunk))
	}
}
