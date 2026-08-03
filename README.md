# fastcdc-go

[![Go Reference](https://pkg.go.dev/badge/github.com/SaveTheRbtz/fastcdc-go.svg)](https://pkg.go.dev/github.com/SaveTheRbtz/fastcdc-go)

`fastcdc-go` splits byte slices and streams into content-defined chunks with the
FastCDC 2020 algorithm. Its validated `Chunker` is immutable and safe for
concurrent use, with small APIs for in-memory and streaming input.

The module requires Go 1.23 or later.

## Install

```sh
go get github.com/SaveTheRbtz/fastcdc-go
```

## Byte slices

Use `Chunks` when the complete input is already in memory:

```go
chunker, err := fastcdc.New(fastcdc.Config{AverageSize: 1 << 20})
if err != nil {
	log.Fatal(err)
}

for offset, chunk := range chunker.Chunks(data) {
	fmt.Printf("offset=%d size=%d\n", offset, len(chunk))
}
```

`Chunks` can be ranged over repeatedly and reads the input during each pass.
Do not mutate the input while ranging. Each chunk aliases the input slice and
has its capacity clipped to its length; mutating either view changes the same
storage.

`Cut(data)` is the lower-level operation. It returns the length of the first
chunk, or zero for empty input. It treats `data` as a complete input, so it
returns a short final chunk immediately. Do not use `Cut` on an incomplete
stream fragment; use `Reader` instead.

To loop over `Cut` manually, process `data[:n]` and continue with `data[n:]`.
`Chunks` and `Reader` handle this for you.

## Streams

Create an independent `Reader` for each stream:

```go
reader := chunker.NewReader(src)
for {
	offset := reader.InputOffset()
	chunk, err := reader.Next()
	if err == io.EOF {
		break
	}
	if err != nil {
		return err
	}

	if err := consume(offset, chunk); err != nil {
		return err
	}
}
```

`Next` returns non-empty chunks and reports `io.EOF` only after returning the
final buffered bytes. The returned slice borrows reader-owned storage and
remains valid only until the next call to `Next` or `Reset`, even if that call
returns an error. Clone a chunk before retaining it:

```go
saved := bytes.Clone(chunk)
```

The reader may consume input beyond the last returned chunk. `InputOffset`
reports the logical end of that chunk, not the underlying source position.
Abandoning the reader or calling `Reset` can therefore discard bytes already
read from the source. The reader never closes its source.

If an underlying `Read` returns bytes and an error together, those bytes are
kept and examined first. `Next` returns any complete chunks before reporting a
non-EOF error. The caller may call `Next` again to resume without losing the
buffered partial chunk. Repeated `(0, nil)` reads eventually produce
`io.ErrNoProgress`; the reader can still be resumed or reset.

`Reader` is not safe for concurrent use and must not be copied. `Reset` reuses
its allocation, discards buffered input and pending errors, and sets
`InputOffset` back to zero.

## Configuration

`AverageSize` is the only required field:

| Field | Rules and default |
| --- | --- |
| `AverageSize` | Power of two from 256 B through 4 MiB; required |
| `MinSize` | Positive and less than `AverageSize`; default `AverageSize / 4` |
| `MaxSize` | Greater than `AverageSize` and at most 16 MiB; default `AverageSize * 4` |
| `Normalization` | Default `NormalizationLevel1` |

`AverageSize` selects the target scale; it does not guarantee the arithmetic
mean of produced chunks. The observed mean also depends on normalization and
input data.

Normalization changes the boundary mask below and above the requested average
so chunk sizes cluster more tightly around it:

| Value | Behavior |
| --- | --- |
| `NormalizationNone` | Disable normalization |
| `NormalizationLevel1` | Moderate normalization; also selected by the zero value |
| `NormalizationLevel2` | Narrower size distribution |
| `NormalizationLevel3` | Narrowest size distribution |

Higher levels trade a tighter distribution for different chunk boundaries.
All configuration fields are part of the chunking format: use the same values
where identical boundaries are required.

A `Chunker` contains only validated, immutable state. Share it freely between
goroutines, and create a separate `Reader` for each stream.

## Command-line tool

Install the included boundary-inspection tool:

```sh
go install github.com/SaveTheRbtz/fastcdc-go/cmd/fastcdc@latest
fastcdc -file archive.img -avg 1048576
```

It prints each chunk's offset and size. Pass `-csv` for CSV output,
`-normalization` for levels 1 through 3, or `-no-normalization` to disable
normalization.

## Analysis

The [reproducible analysis](analysis/README.md) records deterministic
chunk-size distributions, Linux source-history dedup ratios at two chunk
sizes, and an exact source-boundary oracle comparison. The supporting command
emits canonical CSV; plots are generated separately with the checked-in
gnuplot script.

| No normalization | Level 1 |
| --- | --- |
| [![No-normalization distribution](analysis/distribution-64k-none.svg)](analysis/distribution-64k-none.svg) | [![Level-1 distribution](analysis/distribution-64k-n1.svg)](analysis/distribution-64k-n1.svg) |

| Level 2 | Level 3 |
| --- | --- |
| [![Level-2 distribution](analysis/distribution-64k-n2.svg)](analysis/distribution-64k-n2.svg) | [![Level-3 distribution](analysis/distribution-64k-n3.svg)](analysis/distribution-64k-n3.svg) |

## Compatibility

This version implements the 2020 algorithm and its canonical Gear table. It
does not preserve the API or chunk boundaries of earlier `fastcdc-go`
releases.

## References

- Wen Xia et al., [FastCDC: a Fast and Efficient Content-Defined Chunking
  Approach for Data Deduplication](https://doi.org/10.1109/TPDS.2020.2984632),
  IEEE Transactions on Parallel and Distributed Systems, 2020.
- Wen Xia et al., [FastCDC: a Fast and Efficient Content-Defined Chunking
  Approach for Data Deduplication](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf),
  USENIX ATC, 2016.

## License

Apache-2.0. See [LICENSE](LICENSE).
