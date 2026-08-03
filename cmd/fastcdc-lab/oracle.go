package main

import (
	"bytes"
	"index/suffixarray"
)

// targetPartitionOracle compares a fixed set of target chunks with one source
// file. actual counts chunks equal to a source FastCDC chunk. oracle counts
// chunks occurring at any source offset, regardless of source boundaries.
// Unlike the SHA-256 aggregate metrics, both results are byte-exact.
func targetPartitionOracle(source []byte, sourceChunks [][]byte, targetChunks []analyzedChunk) (actual, oracle int64) {
	var index *suffixarray.Index
	if len(source) > 0 {
		index = suffixarray.New(source)
	}
	for _, target := range targetChunks {
		for _, candidate := range sourceChunks {
			if bytes.Equal(candidate, target.data) {
				actual += int64(len(target.data))
				break
			}
		}
		if index != nil && len(target.data) <= len(source) && len(index.Lookup(target.data, 1)) != 0 {
			oracle += int64(len(target.data))
		}
	}
	return actual, oracle
}
