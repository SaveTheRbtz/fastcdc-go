package main

import (
	"bytes"
	"crypto/sha256"
	"index/suffixarray"
)

// targetPartitionOracle compares a fixed set of target chunks with one source
// file. actual counts chunks equal to a source FastCDC chunk. oracle counts
// chunks occurring at any source offset, regardless of source boundaries.
// Unlike the SHA-256 aggregate metrics, both results are byte-exact.
func targetPartitionOracle(source []byte, sourceChunks [][]byte, targetChunks []analyzedChunk) (actual, oracle int64) {
	chunkIndex := make(map[chunkKey][][]byte, len(sourceChunks))
	for _, chunk := range sourceChunks {
		key := chunkKey{digest: sha256.Sum256(chunk), size: uint32(len(chunk))}
		chunkIndex[key] = append(chunkIndex[key], chunk)
	}
	var index *suffixarray.Index
	if len(source) > 0 {
		index = suffixarray.New(source)
	}
	for _, target := range targetChunks {
		key := chunkKey{digest: sha256.Sum256(target.data), size: uint32(len(target.data))}
		for _, candidate := range chunkIndex[key] {
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
