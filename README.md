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

If the source returns a non-EOF error, the reader treats the successfully read
prefix as a complete, truncated stream. `Next` returns its complete chunks and
remaining short tail before returning the source error. The error is terminal:
later calls return the same error until `Reset` selects a new source. Repeated
`(0, nil)` reads eventually produce terminal `io.ErrNoProgress`.

`Reader` is not safe for concurrent use and must not be copied. `Reset` reuses
its chunk buffer, discards buffered input and the terminal error, and sets
`InputOffset` back to zero.

## Configuration

`AverageSize` is the only required field:

| Field | Rules and default |
| --- | --- |
| `AverageSize` | Power of two from 256 B through 4 MiB; required |
| `MinSize` | Positive and less than `AverageSize`; default `AverageSize / 4` |
| `MaxSize` | Greater than `AverageSize` and at most 16 MiB; default `AverageSize * 4` |
| `Normalization` | Default `NormalizationLevel1` |
| `Masks` | Three boundary masks; zero value selects `MasksDefault` |
| `GearTable` | Fixed 256-entry lookup table; zero value selects `GearTableDefault` |

`AverageSize` selects the target scale; it does not guarantee the arithmetic
mean of produced chunks. The observed mean also depends on normalization and
input data.

With derived masks, normalization changes the boundary mask below and above the
requested average so chunk sizes cluster more tightly around it:

| Value | Behavior |
| --- | --- |
| `NormalizationNone` | Disable normalization |
| `NormalizationLevel1` | Moderate normalization; also selected by the zero value |
| `NormalizationLevel2` | Narrower size distribution |
| `NormalizationLevel3` | Narrowest size distribution |

Higher levels trade a tighter distribution for different chunk boundaries.
With custom masks, levels 1 through 3 all use `Small` and `Large`; only
`NormalizationNone`, which uses `Average`, is distinct.

A `Chunker` contains only validated, immutable state. Share it freely between
goroutines, and create a separate `Reader` for each stream.

## Compatibility profiles

`CConfig` returns the complete configuration for the scalar normalized 64-bit
routine in the FastCDC authors' C implementation:

```go
chunker, err := fastcdc.New(fastcdc.CConfig())
```

The root package also provides `GearTableDefault`, `GearTableC`,
`MasksDefault`, and `MasksC` for constructing custom configurations. Their
GoDoc records the source implementations and compatibility constraints.

Gear values, masks, sizes, normalization, rolling-hash semantics, and boundary
conventions are all part of a chunking format. Other FastCDC implementations
may differ in any of them, so copying only a table or mask does not establish
compatibility. Custom values are not statistically validated; poor values can
produce poor chunk-size distributions.

## Performance

On an Intel Core i5-10210U with Go 1.26.5 on Linux/amd64, `Reader` processed a
1,660,723,200-byte tar archive of Linux v7.1 in 1.626 seconds. The configuration
used a 1 MiB average chunk size and the derived defaults: 256 KiB minimum,
4 MiB maximum, and normalization level 1.

The input was in the operating-system page cache. The process was pinned to one
hardware thread with `GOMAXPROCS=1`; the CPU used its performance governor and
the NMI watchdog was disabled. Each of ten samples read the complete archive
once. The benchmark deliberately omitted the race detector, whose
instrumentation changes performance.

| `benchstat` metric | Result |
| --- | ---: |
| Time | 1.626 s/op ± 1% |
| Throughput | 974.2 MiB/s ± 1% |
| Allocated bytes | 272 B/op ± 0% |
| Allocations | 5 allocs/op ± 0% |

`Reader`'s reusable 4 MiB chunk buffer was allocated before measurement. The
reported allocations include opening and closing the input file and resetting
the reader for one complete pass.

The input was the archive of Linux commit
`8cd9520d35a6c38db6567e97dd93b1f11f185dc6` (tag `v7.1`), with SHA-256
`b5470460e136e037c3f90579b6ec9b61c53e6f833b1acc37c0329466ffea0b6e`.
Reproduce the measurement with:

```sh
go install golang.org/x/perf/cmd/benchstat@v0.0.0-20260709024250-82a0b07e230d

git -C "$LINUX_REPO" archive --format=tar \
  --output=/tmp/linux-v7.1.tar \
  8cd9520d35a6c38db6567e97dd93b1f11f185dc6

taskset -c 3 env GOMAXPROCS=1 FASTCDC_BENCH_FILE=/tmp/linux-v7.1.tar \
  go test -run '^$' -bench '^BenchmarkReaderFile$' \
  -benchmem -benchtime=1x -count=10 > reader.bench
benchstat reader.bench
```

## Command-line tool

Install the included boundary-inspection tool:

```sh
go install github.com/SaveTheRbtz/fastcdc-go/cmd/fastcdc@latest
fastcdc -file archive.img -avg 1048576
```

It prints each chunk's offset and size. Pass `-csv` for CSV output,
`-normalization=1`, `2`, or `3` to select a level, or `-normalization=-1` to
disable normalization.

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

With `Masks` and `GearTable` left zero, `New` preserves v0.4.0 chunk
boundaries. These fields are additional parts of the chunking format. This
version does not preserve the API or chunk boundaries of releases before
v0.4.0.

## References

- Wen Xia et al., [FastCDC: a Fast and Efficient Content-Defined Chunking
  Approach for Data Deduplication](https://doi.org/10.1109/TPDS.2020.2984632),
  IEEE Transactions on Parallel and Distributed Systems, 2020.
- Wen Xia et al., [FastCDC: a Fast and Efficient Content-Defined Chunking
  Approach for Data Deduplication](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf),
  USENIX ATC, 2016.

## License

Apache-2.0. See [LICENSE](LICENSE).
