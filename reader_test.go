package fastcdc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"testing"
	"time"
)

func TestReaderMatchesChunksAcrossFragments(t *testing.T) {
	chunker := mustChunker(t, Config{
		MinSize:       65,
		AverageSize:   256,
		MaxSize:       1025,
		Normalization: NormalizationLevel2,
	})
	data := splitMixBytes(256<<10, 0x243f6a8885a308d3)
	want := collectMemoryChunks(chunker, data)

	tests := []struct {
		name  string
		sizes []int
	}{
		{name: "one_byte", sizes: []int{1}},
		{name: "fibonacci", sizes: []int{1, 2, 3, 5, 8, 13, 21, 34, 55, 89}},
		{name: "around_phases", sizes: []int{64, 1, 190, 2, 767, 258, 1024}},
		{name: "large", sizes: []int{4096}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chunker.NewReader(&fragmentReader{data: data, sizes: tt.sizes})
			got := collectReaderChunks(t, r)
			assertChunksEqual(t, got, want)
		})
	}
}

func TestReaderReturnsBoundaryBeforeMaxSize(t *testing.T) {
	chunker := mustChunker(t, Config{
		MinSize:       64,
		AverageSize:   256,
		MaxSize:       1024,
		Normalization: NormalizationNone,
	})
	input := make([]byte, 65)
	input[64] = 0xc0

	pr, pw := io.Pipe()
	release := make(chan struct{})
	t.Cleanup(func() {
		_ = pr.CloseWithError(errors.New("test cleanup"))
		select {
		case <-release:
		default:
			close(release)
		}
	})
	writerDone := make(chan error, 1)
	go func() {
		_, err := pw.Write(input)
		if err == nil {
			<-release
		}
		if closeErr := pw.Close(); err == nil {
			err = closeErr
		}
		writerDone <- err
	}()

	r := chunker.NewReader(pr)
	type result struct {
		chunk []byte
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		chunk, err := r.Next()
		resultCh <- result{chunk: bytes.Clone(chunk), err: err}
	}()

	var got result
	select {
	case got = <-resultCh:
	case <-time.After(10 * time.Second):
		_ = pr.CloseWithError(errors.New("test timeout"))
		close(release)
		<-writerDone
		t.Fatal("Next blocked after a boundary was decidable")
	}

	close(release)
	if err := <-writerDone; err != nil {
		t.Fatalf("pipe writer: %v", err)
	}
	if got.err != nil {
		t.Fatalf("Next() error = %v, want nil", got.err)
	}
	if len(got.chunk) != 64 {
		t.Fatalf("Next() returned %d bytes, want 64", len(got.chunk))
	}
	if offset := r.InputOffset(); offset != 64 {
		t.Fatalf("InputOffset() = %d, want 64", offset)
	}
	tail, err := r.Next()
	if err != nil {
		t.Fatalf("tail Next returned error: %v", err)
	}
	if !bytes.Equal(tail, []byte{0xc0}) {
		t.Fatalf("tail = %x, want c0", tail)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("final Next error = %v, want io.EOF", err)
	}
}

func TestReaderDrainsPrefixBeforeTerminalError(t *testing.T) {
	chunker := mustChunker(t, Config{
		MinSize:       64,
		AverageSize:   256,
		MaxSize:       1024,
		Normalization: NormalizationNone,
	})
	terminal := errors.New("terminal read failure")
	boundaries := make([]byte, 129)
	boundaries[64] = 0xc0
	boundaries[128] = 0xc0
	maximumChunker := mustChunker(t, Config{AverageSize: 256})
	maximum := make([]byte, maximumChunker.maxSize)
	tests := []struct {
		name    string
		chunker *Chunker
		data    []byte
		want    [][]byte
	}{
		{name: "error without data", chunker: chunker},
		{
			name:    "complete chunks and short tail",
			chunker: chunker,
			data:    boundaries,
			want:    [][]byte{boundaries[:64], boundaries[64:128], boundaries[128:]},
		},
		{
			name:    "forced maximum",
			chunker: maximumChunker,
			data:    maximum,
			want:    [][]byte{maximum},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &scriptedReader{steps: []readStep{
				{data: test.data, err: terminal},
				{data: []byte("unreachable"), err: io.EOF},
			}}
			r := test.chunker.NewReader(source)
			var got [][]byte
			for {
				chunk, err := r.Next()
				if err == terminal {
					break
				}
				if err != nil {
					t.Fatalf("Next() error = %v, want %v", err, terminal)
				}
				got = append(got, bytes.Clone(chunk))
			}
			assertChunksEqual(t, got, test.want)
			if got := r.InputOffset(); got != int64(len(test.data)) {
				t.Errorf("InputOffset() = %d, want %d", got, len(test.data))
			}
			if source.index != 1 {
				t.Errorf("source advanced to step %d, want 1", source.index)
			}

			reads := len(source.requests)
			for call := 1; call <= 2; call++ {
				chunk, err := r.Next()
				if chunk != nil || err != terminal {
					t.Errorf("Next() after terminal error, call %d = (%v, %v), want (nil, %v)", call, chunk, err, terminal)
				}
			}
			if got := len(source.requests); got != reads {
				t.Errorf("repeated Next() made %d additional reads, want 0", got-reads)
			}
		})
	}
}

func TestReaderReturnsBufferedDataBeforeEOF(t *testing.T) {
	chunker := mustChunker(t, Config{
		MinSize:       64,
		AverageSize:   256,
		MaxSize:       1024,
		Normalization: NormalizationNone,
	})
	input := make([]byte, 129)
	input[64] = 0xc0
	input[128] = 0xc0
	tests := []struct {
		name string
		data []byte
		want [][]byte
	}{
		{name: "empty"},
		{
			name: "complete chunks and short tail",
			data: input,
			want: [][]byte{input[:64], input[64:128], input[128:]},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := chunker.NewReader(&scriptedReader{steps: []readStep{{data: test.data, err: io.EOF}}})
			got := collectReaderChunks(t, r)
			assertChunksEqual(t, got, test.want)
			if got := r.InputOffset(); got != int64(len(test.data)) {
				t.Errorf("InputOffset() = %d, want %d", got, len(test.data))
			}
			for call := 1; call <= 2; call++ {
				if chunk, err := r.Next(); chunk != nil || err != io.EOF {
					t.Errorf("Next() after EOF, call %d = (%v, %v), want (nil, io.EOF)", call, chunk, err)
				}
			}
		})
	}
}

func TestReaderResetDiscardsStateAndReusesBuffer(t *testing.T) {
	chunker := mustChunker(t, Config{
		MinSize:       64,
		AverageSize:   256,
		MaxSize:       1024,
		Normalization: NormalizationNone,
	})
	terminal := errors.New("discard me")
	old := make([]byte, 65)
	old[64] = 0xc0
	r := chunker.NewReader(&scriptedReader{steps: []readStep{{data: old, err: terminal}}})
	buffer := &r.buffer[0]
	if _, err := r.Next(); err != nil {
		t.Fatalf("old stream chunk: %v", err)
	}

	fresh := []byte{4, 5, 6}
	r.Reset(bytes.NewReader(fresh))
	if &r.buffer[0] != buffer {
		t.Error("Reset() replaced the reusable buffer")
	}
	if got := r.InputOffset(); got != 0 {
		t.Errorf("InputOffset() after Reset() = %d, want 0", got)
	}
	chunk, err := r.Next()
	if err != nil {
		t.Fatalf("Next() after Reset() error = %v, want nil", err)
	}
	if !bytes.Equal(chunk, fresh) {
		t.Errorf("Next() after Reset() = %v, want %v", chunk, fresh)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Errorf("Next() after fresh stream = %v, want io.EOF", err)
	}

	r.Reset(&scriptedReader{steps: []readStep{{err: terminal}}})
	if chunk, err := r.Next(); chunk != nil || err != terminal {
		t.Fatalf("Next() on terminal stream = (%v, %v), want (nil, %v)", chunk, err, terminal)
	}
	again := []byte{7, 8}
	r.Reset(bytes.NewReader(again))
	chunk, err = r.Next()
	if err != nil || !bytes.Equal(chunk, again) {
		t.Errorf("Next() after resetting terminal error = (%v, %v), want (%v, nil)", chunk, err, again)
	}
}

func TestReaderOffsetUsesInt64(t *testing.T) {
	chunker := mustChunker(t, Config{AverageSize: 256})
	r := chunker.NewReader(bytes.NewReader([]byte{1}))
	r.offset = math.MaxInt32
	if _, err := r.Next(); err != nil {
		t.Fatalf("Next() error = %v, want nil", err)
	}
	if got, want := r.InputOffset(), int64(math.MaxInt32)+1; got != want {
		t.Fatalf("InputOffset = %d, want %d", got, want)
	}
}

func TestReaderRejectsNilSource(t *testing.T) {
	chunker := mustChunker(t, Config{AverageSize: 256})
	r := chunker.NewReader(bytes.NewReader(nil))
	tests := []struct {
		name string
		call func()
	}{
		{name: "NewReader", call: func() { chunker.NewReader(nil) }},
		{name: "Reset", call: func() { r.Reset(nil) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("%s(nil) did not panic", test.name)
				}
			}()
			test.call()
		})
	}
}

func TestChunkerConcurrentUse(t *testing.T) {
	chunker := mustChunker(t, Config{AverageSize: 1024})
	data := splitMixBytes(256<<10, 0x243f6a8885a308d3)
	want := cutLengths(chunker, data)
	for worker := range 4 {
		t.Run(fmt.Sprintf("worker %d", worker), func(t *testing.T) {
			t.Parallel()
			for range 3 {
				if got := cutLengths(chunker, data); !slices.Equal(got, want) {
					t.Fatalf("Chunks() lengths = %v, want %v", got, want)
				}
				if got := chunker.Cut(data); got != want[0] {
					t.Fatalf("Cut() = %d, want %d", got, want[0])
				}
			}
		})
	}
}

func collectMemoryChunks(chunker *Chunker, data []byte) [][]byte {
	var chunks [][]byte
	for _, chunk := range chunker.Chunks(data) {
		chunks = append(chunks, bytes.Clone(chunk))
	}
	return chunks
}

func collectReaderChunks(t *testing.T, r *Reader) [][]byte {
	t.Helper()
	var chunks [][]byte
	var offset int64
	for {
		if got := r.InputOffset(); got != offset {
			t.Fatalf("InputOffset() = %d, want %d", got, offset)
		}
		chunk, err := r.Next()
		if err == io.EOF {
			if chunk != nil {
				t.Errorf("Next() at EOF returned %d bytes, want nil", len(chunk))
			}
			return chunks
		}
		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if len(chunk) == 0 {
			t.Fatal("Next() returned an empty chunk")
		}
		if cap(chunk) != len(chunk) {
			t.Fatalf("Next() returned len=%d cap=%d, want clipped capacity", len(chunk), cap(chunk))
		}
		chunks = append(chunks, bytes.Clone(chunk))
		offset += int64(len(chunk))
	}
}

func chunkLengths(chunks [][]byte) []int {
	lengths := make([]int, len(chunks))
	for i, chunk := range chunks {
		lengths[i] = len(chunk)
	}
	return lengths
}

func assertChunksEqual(t testing.TB, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("chunk count = %d, want %d; lengths %v, want %v", len(got), len(want), chunkLengths(got), chunkLengths(want))
	}
	offset := 0
	for i := range want {
		if bytes.Equal(got[i], want[i]) {
			offset += len(want[i])
			continue
		}
		firstDifference := min(len(got[i]), len(want[i]))
		for j := 0; j < firstDifference; j++ {
			if got[i][j] != want[i][j] {
				firstDifference = j
				break
			}
		}
		t.Fatalf("chunk %d at stream offset %d differs: len=%d, want %d; first difference at byte %d",
			i, offset, len(got[i]), len(want[i]), firstDifference)
	}
}

type fragmentReader struct {
	data  []byte
	sizes []int
	index int
	off   int
}

func (r *fragmentReader) Read(p []byte) (int, error) {
	if r.off == len(r.data) {
		return 0, io.EOF
	}
	size := r.sizes[r.index%len(r.sizes)]
	r.index++
	size = min(size, len(p), len(r.data)-r.off)
	n := copy(p, r.data[r.off:r.off+size])
	r.off += n
	return n, nil
}

type readStep struct {
	data []byte
	err  error
}

type scriptedReader struct {
	steps    []readStep
	index    int
	off      int
	requests []int
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	r.requests = append(r.requests, len(p))
	if r.index == len(r.steps) {
		return 0, io.EOF
	}
	step := &r.steps[r.index]
	n := copy(p, step.data[r.off:])
	r.off += n
	if r.off < len(step.data) {
		return n, nil
	}
	r.index++
	r.off = 0
	return n, step.err
}
