package fastcdc

import (
	"bytes"
	"errors"
	"io"
	"math"
	"slices"
	"testing"
	"time"
)

func TestReaderMatchesChunksAcrossFragments(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{
		MinSize:       65,
		AverageSize:   256,
		MaxSize:       1025,
		Normalization: NormalizationLevel2,
	})
	data := readerTestData(256 << 10)
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
			if !slices.EqualFunc(got, want, bytes.Equal) {
				t.Fatalf("stream chunks differ from in-memory chunks: got %v, want %v", chunkLengths(got), chunkLengths(want))
			}
		})
	}
}

func TestReaderReturnsBoundaryBeforeMaxSize(t *testing.T) {
	t.Parallel()

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

	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("Next returned error: %v", got.err)
		}
		if len(got.chunk) != 64 {
			t.Fatalf("Next returned %d bytes, want 64", len(got.chunk))
		}
		if r.InputOffset() != 64 {
			t.Fatalf("InputOffset = %d, want 64", r.InputOffset())
		}
	case <-time.After(10 * time.Second):
		_ = pr.CloseWithError(errors.New("test timeout"))
		close(release)
		t.Fatal("Next blocked after a boundary was decidable")
	}

	close(release)
	if err := <-writerDone; err != nil {
		t.Fatalf("pipe writer: %v", err)
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
	t.Parallel()

	chunker := mustChunker(t, Config{
		MinSize:       64,
		AverageSize:   256,
		MaxSize:       1024,
		Normalization: NormalizationNone,
	})
	terminal := errors.New("terminal read failure")
	first := make([]byte, 129)
	first[64] = 0xc0
	first[128] = 0xc0
	source := &scriptedReader{steps: []readStep{
		{data: first, err: terminal},
		{data: []byte{1, 2}, err: io.EOF},
	}}
	r := chunker.NewReader(source)

	want := [][]byte{first[:64], first[64:128], first[128:]}
	for i, expected := range want {
		offset := int64(i * 64)
		if got := r.InputOffset(); got != offset {
			t.Fatalf("chunk %d: InputOffset = %d, want %d", i, got, offset)
		}
		chunk, err := r.Next()
		if err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		if !bytes.Equal(chunk, expected) {
			t.Fatalf("chunk %d = %x, want %x", i, chunk, expected)
		}
	}
	if r.InputOffset() != int64(len(first)) {
		t.Fatalf("offset before error = %d, want %d", r.InputOffset(), len(first))
	}
	reads := len(source.requests)
	for call := 1; call <= 3; call++ {
		chunk, err := r.Next()
		if chunk != nil || err != terminal {
			t.Fatalf("terminal Next call %d = (%v, %v), want (nil, terminal)", call, chunk, err)
		}
		if r.InputOffset() != int64(len(first)) {
			t.Fatalf("offset after error call %d = %d, want %d", call, r.InputOffset(), len(first))
		}
		if len(source.requests) != reads {
			t.Fatalf("terminal Next call %d read the source again", call)
		}
	}
	if source.index != 1 {
		t.Fatalf("source advanced to step %d, want 1", source.index)
	}
}

func TestReaderZeroByteErrorIsTerminal(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	terminal := errors.New("terminal failure")
	data := []byte{1, 2, 3}
	source := &scriptedReader{steps: []readStep{
		{err: terminal},
		{data: data, err: io.EOF},
	}}
	r := chunker.NewReader(source)

	for call := 1; call <= 3; call++ {
		chunk, err := r.Next()
		if chunk != nil || err != terminal {
			t.Fatalf("Next call %d = (%v, %v), want (nil, terminal)", call, chunk, err)
		}
		if r.InputOffset() != 0 {
			t.Fatalf("InputOffset after error call %d = %d, want 0", call, r.InputOffset())
		}
		if len(source.requests) != 1 {
			t.Fatalf("Next call %d made %d reads, want 1", call, len(source.requests))
		}
	}
	if source.index != 1 {
		t.Fatalf("source advanced to step %d, want 1", source.index)
	}
}

func TestReaderReturnsForcedMaximumBeforeTerminalError(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	terminal := errors.New("failure at maximum")
	data := make([]byte, chunker.maxSize)
	source := &scriptedReader{steps: []readStep{{data: data, err: terminal}}}
	r := chunker.NewReader(source)

	chunk, err := r.Next()
	if err != nil || !bytes.Equal(chunk, data) {
		t.Fatalf("first Next = (%d bytes, %v), want (%d bytes, nil)", len(chunk), err, len(data))
	}
	if r.InputOffset() != int64(chunker.maxSize) {
		t.Fatalf("InputOffset = %d, want %d", r.InputOffset(), chunker.maxSize)
	}
	reads := len(source.requests)
	for call := 1; call <= 3; call++ {
		chunk, err := r.Next()
		if chunk != nil || err != terminal {
			t.Fatalf("terminal Next call %d = (%v, %v), want (nil, terminal)", call, chunk, err)
		}
		if r.InputOffset() != int64(chunker.maxSize) {
			t.Fatalf("InputOffset after error call %d = %d, want %d", call, r.InputOffset(), chunker.maxSize)
		}
		if len(source.requests) != reads {
			t.Fatalf("terminal Next call %d read the source again", call)
		}
	}
}

func TestReaderProcessesDataBeforeEOF(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{
		MinSize:       64,
		AverageSize:   256,
		MaxSize:       1024,
		Normalization: NormalizationNone,
	})
	input := make([]byte, 129)
	input[64] = 0xc0
	input[128] = 0xc0
	r := chunker.NewReader(&scriptedReader{steps: []readStep{{data: input, err: io.EOF}}})

	got := collectReaderChunks(t, r)
	want := [][]byte{input[:64], input[64:128], input[128:]}
	if !slices.EqualFunc(got, want, bytes.Equal) {
		t.Fatalf("chunks = %v, want %v", chunkLengths(got), chunkLengths(want))
	}
	for call := 1; call <= 2; call++ {
		if chunk, err := r.Next(); chunk != nil || err != io.EOF {
			t.Fatalf("Next after EOF call %d = (%v, %v), want (nil, io.EOF)", call, chunk, err)
		}
	}
}

func TestReaderEmptyStreamReturnsRepeatedEOF(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	source := &scriptedReader{}
	r := chunker.NewReader(source)
	for call := 1; call <= 3; call++ {
		chunk, err := r.Next()
		if chunk != nil || err != io.EOF {
			t.Fatalf("Next call %d = (%v, %v), want (nil, io.EOF)", call, chunk, err)
		}
		if r.InputOffset() != 0 {
			t.Fatalf("InputOffset after EOF call %d = %d, want 0", call, r.InputOffset())
		}
		if len(source.requests) != 1 {
			t.Fatalf("Next call %d made %d reads, want 1", call, len(source.requests))
		}
	}
}

func TestReaderNoProgressIsTerminalUntilReset(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	source := &emptyReader{}
	r := chunker.NewReader(source)
	reads := 0
	for call := 1; call <= 3; call++ {
		chunk, err := r.Next()
		if chunk != nil || err != io.ErrNoProgress {
			t.Fatalf("Next call %d = (%v, %v), want (nil, io.ErrNoProgress)", call, chunk, err)
		}
		if r.InputOffset() != 0 {
			t.Fatalf("InputOffset after error call %d = %d, want 0", call, r.InputOffset())
		}
		if call == 1 {
			reads = source.reads
			if reads == 0 {
				t.Fatal("Next returned io.ErrNoProgress without reading the source")
			}
		} else if source.reads != reads {
			t.Fatalf("Next call %d made %d additional reads", call, source.reads-reads)
		}
	}

	fresh := []byte{4, 5, 6}
	r.Reset(bytes.NewReader(fresh))
	if r.InputOffset() != 0 {
		t.Fatalf("InputOffset after Reset = %d, want 0", r.InputOffset())
	}
	chunk, err := r.Next()
	if err != nil || !bytes.Equal(chunk, fresh) {
		t.Fatalf("Next after Reset = (%v, %v), want (%v, nil)", chunk, err, fresh)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("final error after Reset = %v, want io.EOF", err)
	}
}

func TestReaderResetDiscardsStateAndReusesBuffer(t *testing.T) {
	t.Parallel()

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
		t.Fatal("Reset replaced the reusable buffer")
	}
	if r.InputOffset() != 0 {
		t.Fatalf("offset after Reset = %d, want 0", r.InputOffset())
	}
	chunk, err := r.Next()
	if err != nil {
		t.Fatalf("fresh stream: %v", err)
	}
	if !bytes.Equal(chunk, fresh) {
		t.Fatalf("fresh chunk = %v, want %v", chunk, fresh)
	}
}

func TestReaderResetAfterSurfacedError(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	terminal := errors.New("surface me")
	r := chunker.NewReader(&scriptedReader{steps: []readStep{{err: terminal}}})
	if chunk, err := r.Next(); chunk != nil || err != terminal {
		t.Fatalf("old stream Next = (%v, %v), want (nil, terminal)", chunk, err)
	}
	if chunk, err := r.Next(); chunk != nil || err != terminal {
		t.Fatalf("repeated old stream Next = (%v, %v), want (nil, terminal)", chunk, err)
	}

	fresh := []byte{4, 5, 6}
	r.Reset(bytes.NewReader(fresh))
	if r.InputOffset() != 0 {
		t.Fatalf("InputOffset after Reset = %d, want 0", r.InputOffset())
	}
	chunk, err := r.Next()
	if err != nil || !bytes.Equal(chunk, fresh) {
		t.Fatalf("fresh stream Next = (%v, %v), want (%v, nil)", chunk, err, fresh)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("fresh stream final error = %v, want io.EOF", err)
	}
}

func TestReaderResetAfterEOF(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	r := chunker.NewReader(bytes.NewReader([]byte{1}))
	if chunk, err := r.Next(); err != nil || !bytes.Equal(chunk, []byte{1}) {
		t.Fatalf("old stream Next = (%v, %v), want ([1], nil)", chunk, err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("old stream final error = %v, want io.EOF", err)
	}

	fresh := []byte{2, 3}
	r.Reset(bytes.NewReader(fresh))
	if r.InputOffset() != 0 {
		t.Fatalf("InputOffset after Reset = %d, want 0", r.InputOffset())
	}
	chunk, err := r.Next()
	if err != nil || !bytes.Equal(chunk, fresh) {
		t.Fatalf("fresh stream Next = (%v, %v), want (%v, nil)", chunk, err, fresh)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("fresh stream final error = %v, want io.EOF", err)
	}
}

func TestReaderOffsetUsesInt64(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	r := chunker.NewReader(bytes.NewReader([]byte{1}))
	r.offset = math.MaxInt32
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if got, want := r.InputOffset(), int64(math.MaxInt32)+1; got != want {
		t.Fatalf("InputOffset = %d, want %d", got, want)
	}
}

func TestReaderChunksHaveClippedCapacity(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	r := chunker.NewReader(bytes.NewReader(readerTestData(4096)))
	for {
		chunk, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if cap(chunk) != len(chunk) {
			t.Fatalf("len=%d cap=%d", len(chunk), cap(chunk))
		}
	}
}

func TestReaderRejectsNilSource(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	assertPanics(t, func() { chunker.NewReader(nil) })
	r := chunker.NewReader(bytes.NewReader(nil))
	assertPanics(t, func() { r.Reset(nil) })
}

func TestReaderRejectsInvalidReadCounts(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 256})
	tests := []struct {
		name string
		read func([]byte) (int, error)
	}{
		{
			name: "negative",
			read: func([]byte) (int, error) { return -1, nil },
		},
		{
			name: "larger than buffer",
			read: func(p []byte) (int, error) { return len(p) + 1, nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0
			r := chunker.NewReader(readerFunc(func(p []byte) (int, error) {
				reads++
				return tt.read(p)
			}))
			var firstErr error
			for call := 1; call <= 3; call++ {
				chunk, err := r.Next()
				if chunk != nil || err == nil {
					t.Fatalf("Next call %d = (%v, %v), want (nil, non-nil error)", call, chunk, err)
				}
				if call == 1 {
					firstErr = err
				} else if err != firstErr {
					t.Fatalf("Next call %d error = %v, want sticky error %v", call, err, firstErr)
				}
				if r.InputOffset() != 0 {
					t.Fatalf("InputOffset after call %d = %d, want 0", call, r.InputOffset())
				}
				if reads != 1 {
					t.Fatalf("Next call %d made %d reads, want 1", call, reads)
				}
			}
		})
	}
}

func TestChunkerConcurrentUse(t *testing.T) {
	t.Parallel()

	chunker := mustChunker(t, Config{AverageSize: 1024})
	data := readerTestData(1 << 20)
	want := cutLengths(chunker, data)
	errCh := make(chan error, 8)
	for worker := 0; worker < cap(errCh); worker++ {
		go func() {
			for iteration := 0; iteration < 20; iteration++ {
				if got := cutLengths(chunker, data); !slices.Equal(got, want) {
					errCh <- errors.New("concurrent Chunks result changed")
					return
				}
				if got := chunker.Cut(data); got != want[0] {
					errCh <- errors.New("concurrent Cut result changed")
					return
				}
			}
			errCh <- nil
		}()
	}
	for worker := 0; worker < cap(errCh); worker++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
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
			t.Fatalf("InputOffset = %d, want %d", got, offset)
		}
		chunk, err := r.Next()
		if err == io.EOF {
			return chunks
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if len(chunk) == 0 {
			t.Fatal("Next returned an empty chunk")
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

func readerTestData(size int) []byte {
	data := make([]byte, size)
	var state uint64 = 0x243f6a8885a308d3
	for i := range data {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		data[i] = byte(state)
	}
	return data
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

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}

type emptyReader struct {
	reads int
}

func (r *emptyReader) Read([]byte) (int, error) {
	r.reads++
	return 0, nil
}

func assertPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("call did not panic")
		}
	}()
	f()
}
