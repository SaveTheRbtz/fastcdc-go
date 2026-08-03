package fastcdc

import (
	"bufio"
	"io"
)

// Reader chunks one byte stream. A Reader must not be copied and is not safe
// for concurrent use. Its zero value is not usable; construct one with
// Chunker.NewReader.
//
// Reader may read ahead. Abandoning it or calling Reset can discard bytes that
// were consumed from the source but not yet returned in a chunk. A non-EOF
// error terminates the stream; Reader never closes its source.
type Reader struct {
	chunker *Chunker
	scanner *bufio.Scanner
	buffer  []byte
	offset  int64
	scan    scanState
}

// Next returns the next non-empty chunk with a nil error. It returns [io.EOF]
// only after all buffered data has been returned, and never returns a chunk
// together with a non-nil error.
//
// A non-EOF error is terminal. Next first returns every chunk from the
// successfully read prefix, including its final short chunk, then returns the
// error on every call until [Reader.Reset].
//
// The returned slice aliases reader-owned storage and is valid only until the
// next call to Next or [Reader.Reset], even when that call returns an error.
// Copy the bytes before retaining them. Its capacity is clipped to its length.
func (r *Reader) Next() ([]byte, error) {
	if r.scanner.Scan() {
		chunk := r.scanner.Bytes()
		r.offset += int64(len(chunk))
		return chunk, nil
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (r *Reader) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) == 0 {
		return 0, nil, nil
	}
	if cut := r.chunker.scan(data, len(data), &r.scan); cut >= 0 {
		r.scan.reset(r.chunker.minSize)
		return cut, data[:cut:cut], nil
	}
	if len(data) == r.chunker.maxSize || atEOF {
		r.scan.reset(r.chunker.minSize)
		return len(data), data[:len(data):len(data)], nil
	}
	return 0, nil, nil
}

// InputOffset returns the byte offset at which the next chunk begins. It
// advances only when [Reader.Next] returns a chunk, excludes read-ahead, and is
// unchanged when Next returns an error.
func (r *Reader) InputOffset() int64 {
	return r.offset
}

// Reset discards buffered input and any terminal error, selects src as the new
// source, and resets [Reader.InputOffset] to zero. It reuses the chunk buffer
// and panics if src is nil.
func (r *Reader) Reset(src io.Reader) {
	if src == nil {
		panic("fastcdc: nil io.Reader")
	}

	chunker := r.chunker
	buffer := r.buffer
	scanner := bufio.NewScanner(src)
	scanner.Buffer(buffer, chunker.maxSize)
	scanner.Split(r.split)

	*r = Reader{
		chunker: chunker,
		scanner: scanner,
		buffer:  buffer,
		scan:    scanState{position: chunker.minSize},
	}
}
