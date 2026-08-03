package fastcdc

import (
	"fmt"
	"io"
)

const maxConsecutiveEmptyReads = 100

// Reader chunks one byte stream. A Reader must not be copied and is not safe
// for concurrent use. Its zero value is not usable; construct one with
// Chunker.NewReader.
//
// Reader may read ahead. Abandoning it or calling Reset can discard bytes that
// were consumed from the source but not yet returned in a chunk. Reader never
// closes its source.
type Reader struct {
	chunker *Chunker
	source  io.Reader
	buffer  []byte
	start   int
	end     int
	offset  int64
	scan    scanState
	pending error
	eof     bool
}

// Next returns the next non-empty chunk. It returns io.EOF only after all
// buffered data has been returned.
//
// The returned slice aliases Reader's buffer and is valid only until the next
// call to Next or Reset, even when that call returns an error. Use bytes.Clone
// to retain it. Its capacity is clipped to its length.
func (r *Reader) Next() ([]byte, error) {
	emptyReads := 0

	for {
		buffered := r.buffer[r.start:r.end]
		if cut := r.chunker.scan(buffered, len(buffered), &r.scan); cut >= 0 {
			return r.emit(cut), nil
		}
		if len(buffered) == len(r.buffer) {
			return r.emit(len(buffered)), nil
		}
		if r.eof {
			if len(buffered) > 0 {
				return r.emit(len(buffered)), nil
			}
			return nil, io.EOF
		}
		if r.pending != nil {
			err := r.pending
			r.pending = nil
			return nil, err
		}

		r.makeRoom()
		space := r.buffer[r.end:]
		n, err := r.source.Read(space)
		if n < 0 || n > len(space) {
			return nil, fmt.Errorf("fastcdc: reader returned invalid byte count %d", n)
		}
		r.end += n

		if n > 0 {
			emptyReads = 0
		} else if err == nil {
			emptyReads++
			if emptyReads >= maxConsecutiveEmptyReads {
				r.pending = io.ErrNoProgress
			}
		} else {
			emptyReads = 0
		}

		switch err {
		case nil:
		case io.EOF:
			r.eof = true
		default:
			r.pending = err
		}
	}
}

// InputOffset returns the logical end of the last successfully returned chunk
// and the start of the next one. It excludes read-ahead and is unchanged when
// Next returns an error.
func (r *Reader) InputOffset() int64 {
	return r.offset
}

// Reset discards buffered input and errors, resets InputOffset to zero, and
// starts reading from src while retaining Reader's buffer. It panics if src is
// nil.
func (r *Reader) Reset(src io.Reader) {
	if src == nil {
		panic("fastcdc: nil io.Reader")
	}
	r.source = src
	r.start = 0
	r.end = 0
	r.offset = 0
	r.pending = nil
	r.eof = false
	r.scan.reset(r.chunker.minSize)
}

func (r *Reader) makeRoom() {
	if r.start == r.end {
		r.start = 0
		r.end = 0
		return
	}
	if r.end == len(r.buffer) {
		r.end = copy(r.buffer, r.buffer[r.start:r.end])
		r.start = 0
	}
}

func (r *Reader) emit(length int) []byte {
	end := r.start + length
	chunk := r.buffer[r.start:end:end]
	r.start = end
	r.offset += int64(length)
	r.scan.reset(r.chunker.minSize)
	return chunk
}
