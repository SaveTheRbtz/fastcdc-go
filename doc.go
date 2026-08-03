// Package fastcdc splits byte sequences and streams into content-defined
// chunks with the FastCDC 2020 algorithm.
//
// Construct a validated, immutable [Chunker] with [New]. A Chunker is safe for
// concurrent use. Its configuration determines the chunking format: identical
// input and configuration produce identical boundaries.
//
// [Chunker.Cut] and [Chunker.Chunks] operate on complete byte slices. Chunks
// returned by Chunker.Chunks alias the input. For incremental input, create an
// independent [Reader] with [Chunker.NewReader]. Reader preserves scan state
// across source reads, so boundaries do not depend on how the stream is
// fragmented.
//
// Slices returned by [Reader.Next] borrow Reader's reusable buffer. They are
// valid only until the next call to Reader.Next or [Reader.Reset], including
// when that call returns an error. Reader may read ahead and never closes its
// source.
package fastcdc
