// Package fastcdc splits byte slices and streams into content-defined chunks
// using the FastCDC 2020 algorithm.
//
// [New] returns an immutable [Chunker] that is safe for concurrent use. The
// same input and [Config] produce the same boundaries.
//
// Use [Chunker.Chunks] or [Chunker.Cut] for complete byte slices and
// [Chunker.NewReader] for streams. The slice and stream APIs produce identical
// boundaries regardless of how a source groups its Read calls.
//
// Chunks from [Chunker.Chunks] alias the input. Chunks from [Reader.Next] alias
// reader-owned storage and remain valid only until the next call to Reader.Next
// or [Reader.Reset]. A Reader may read ahead and never closes its source.
package fastcdc
