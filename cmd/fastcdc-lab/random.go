package main

import (
	"encoding/binary"
	"io"
)

// splitMixReader is a reproducible byte stream. Its output does not depend on
// the sizes of the Read calls made by its consumer.
type splitMixReader struct {
	state     uint64
	pending   [8]byte
	pendingAt int
}

func newSplitMixReader(seed uint64) *splitMixReader {
	return &splitMixReader{state: seed, pendingAt: len([8]byte{})}
}

func (r *splitMixReader) Read(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if r.pendingAt == len(r.pending) {
			r.state += 0x9e3779b97f4a7c15
			z := r.state
			z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
			z = (z ^ (z >> 27)) * 0x94d049bb133111eb
			z ^= z >> 31
			binary.LittleEndian.PutUint64(r.pending[:], z)
			r.pendingAt = 0
		}
		n := copy(p, r.pending[r.pendingAt:])
		r.pendingAt += n
		written += n
		p = p[n:]
	}
	return written, nil
}

type repeatingReader struct {
	pattern   []byte
	remaining int64
	position  int
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	written := 0
	for len(p) > 0 {
		n := copy(p, r.pattern[r.position:])
		written += n
		p = p[n:]
		r.position += n
		if r.position == len(r.pattern) {
			r.position = 0
		}
	}
	r.remaining -= int64(written)
	return written, nil
}
