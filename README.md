# FastCDC-Go

[![Docs](https://godoc.org/github.com/SaveTheRbtz/fastcdc-go?status.svg)](https://pkg.go.dev/github.com/SaveTheRbtz/fastcdc-go?tab=doc)

FastCDC-Go is a Go library implementing the [FastCDC](#references) content-defined chunking algorithm.

Install: 
```
go get -u github.com/SaveTheRbtz/fastcdc-go
```

## Example

```go
import (
  "bytes"
  "fmt"
  "log"
  "math/rand"
  "io"

  "github.com/SaveTheRbtz/fastcdc-go"
)

opts := fastcdc.Options{
  MinSize:     256 * 1024
  AverageSize: 1 * 1024 * 1024
  MaxSize:     4 * 1024 * 1024
}

data := make([]byte, 10 * 1024 * 1024)
rand.Read(data)
chunker, _ := fastcdc.NewChunker(bytes.NewReader(data), opts)

for {
  chunk, err := chunker.Next()
  if err == io.EOF {
    break
  }
  if err != nil {
    log.Fatal(err)
  }

  fmt.Printf("%x  %d\n", chunk.Data[:10], chunk.Length)
}
```

## Command line tool

This package also includes a useful CLI for testing the chunking output. Install it by running:

```
go install ./cmd/fastcdc
```

Example:
```bash
# Outputs the position and size of each chunk to stdout 
fastcdc -csv -file random.txt
```

## Performance

FastCDC-Go is fast. Chunking speed on an i5-10210U is >1GiB/s:
```
name                speed
FastCDCSize/1k-8    19.7GB/s ± 2%
FastCDCSize/4k-8    53.3GB/s ± 2%
FastCDCSize/16k-8   43.1GB/s ±11%
FastCDCSize/32k-8   39.6GB/s ± 6%
FastCDCSize/64k-8   39.4GB/s ± 6%
FastCDCSize/128k-8  31.9GB/s ± 8%
FastCDCSize/256k-8  22.6GB/s ± 3%
FastCDCSize/512k-8  2.22GB/s ±13%
FastCDCSize/1M-8    1.51GB/s ± 3%
FastCDCSize/4M-8    1.23GB/s ± 4%
FastCDCSize/16M-8   1.32GB/s ±10%
FastCDCSize/32M-8   1.30GB/s ± 5%
FastCDCSize/64M-8   1.34GB/s ± 0%
FastCDCSize/128M-8  1.30GB/s ± 5%
FastCDCSize/512M-8  1.30GB/s ± 6%
FastCDCSize/1G-8    1.34GB/s ± 8%
```

## Normalization

A key feature of FastCDC is chunk size normalization. Normalization helps to improve the distribution of chunk sizes, increasing the number of chunks close to the target average size and reducing the number of chunks clipped by the maximum chunk size, as compared to the [Rabin-based](https://en.wikipedia.org/wiki/Rabin_fingerprint) chunking algorithm used in `restic/chunker`.

The histograms below show the chunk size distribution for `fastcdc-go` and `restic/chunker` on 1GiB of random data, each with an average chunk size of 1MiB, a minimum chunk size of 256 KiB and a maximum chunk size of 4MiB. The normalization level for `fastcdc-go` is set to 2.

![](./img/fastcdcgo_norm2_dist.png) ![](./img/restic_dist.png)

Compared to the `restic/chunker`, the distribution of `fastcdc-go` is less skewed (standard deviation 345KiB vs. 964KiB).

## License

FastCDC-Go is licensed under the Apache 2.0 License. See [LICENSE](./LICENSE) for details.

## References

  - Xia, Wen, et al. "Fastcdc: a fast and efficient content-defined chunking approach for data deduplication." 2016 USENIX Annual Technical Conference
  [pdf](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)

