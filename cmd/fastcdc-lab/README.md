# fastcdc-lab

`fastcdc-lab` contains reproducible experiments for the FastCDC 2020
implementation. It is not an end-user file chunking command. Inputs are
deterministic where possible, large runs are opt-in, and CSV/SVG output has no
timestamps so that results remain useful in source-control diffs.

All sizes accept bytes or `KiB`, `MiB`, and `GiB` suffixes. The default chunk
format is average 8 KiB, minimum 2 KiB, maximum 32 KiB, normalization level 1.
Every subcommand also accepts `-average`, `-min`, `-max`, and
`-normalization`.

## Throughput

Run three streaming passes over 1 GiB of deterministic data:

```sh
go run ./cmd/fastcdc-lab bench \
  -bytes 1GiB -corpus 64MiB -rounds 3 -seed 1
```

The corpus is generated before timing and repeated without allocating the full
logical input. The reported boundary digest covers the chunk lengths and must
match across rounds. The timing includes copying from the in-memory source and
the `Reader` API; it is not a storage or hashing benchmark.

Benchmark an existing file, reopening it before every measured round:

```sh
go run ./cmd/fastcdc-lab bench \
  -file /path/to/large.pack -rounds 3
```

File mode reports the exact byte count. By default it first performs two
untimed reads: one direct SHA-256 pass and one pass that hashes the bytes
returned by FastCDC. The hashes and reconstructed byte counts must agree before
timing starts. Use `-verify=false` only when that separate correctness pass has
already succeeded. Timed rounds still compare their chunk-boundary digest.

## Chunk-size distribution

Generate a histogram, an independent-uniform-hash model, and a simple SVG:

```sh
go run ./cmd/fastcdc-lab distribution \
  -bytes 1GiB -seed 1 \
  -csv results/distribution.csv \
  -svg results/distribution.svg
```

The final EOF-shortened chunk is excluded from the histogram. A final chunk
whose length is exactly the configured maximum is retained because it is a
full forced chunk. The analytical curve is the two-hazard model implied by the
small and large FastCDC masks; it is a model, not an assertion that successive
Gear hashes are statistically independent.

## Deduplication

Analyze directories in positional order:

```sh
go run ./cmd/fastcdc-lab dedup -csv dedup.csv snapshot-v1 snapshot-v2
```

Analyze Git revisions without checking them out:

```sh
go run ./cmd/fastcdc-lab dedup \
  -repo /home/rbtz/porn/linux \
  -rev v7.0 -rev v7.1 \
  -csv linux-dedup.csv
```

Git trees are read with `git ls-tree` and one `git cat-file --batch` process.
Only regular blobs are included. Directory symlinks and special files are
skipped. Each regular file is chunked independently, so boundaries cannot
cross file edges.

The report keeps these metrics separate:

- `earlier_snapshot_chunk_reuse_bytes` is an operational, global-history
  metric. It counts target bytes in chunks seen in any earlier snapshot.
  Repetitions first seen within the same snapshot do not inflate this number.
- `unique_chunk_bytes` is the storage needed by one copy of every FastCDC
  chunk across all snapshots.
- `earlier_snapshot_exact_file_reuse_bytes` and `unique_exact_file_bytes` are a
  whole-file baseline. They do not credit partial-file matches.
- Content identity is SHA-256 plus length. The command does not byte-compare
  digest collisions.

For every snapshot after the first, the report also separates target files
into unchanged same-path content, changed same-path content, and new paths.
`changed_same_path_CDC_reuse_bytes` excludes unchanged files, whose often large
reuse rate would otherwise hide boundary behavior on edited content.

An optional, deliberately limited oracle can show how much reuse source chunk
boundaries leave on the table for the target's existing FastCDC partition:

```sh
go run ./cmd/fastcdc-lab dedup \
  -repo /home/rbtz/porn/linux -rev v7.0 -rev v7.1 \
  -oracle -oracle-max-file 1MiB
```

For every FastCDC chunk in an eligible changed same-path target file, a suffix
array asks whether the exact bytes occur anywhere in that file in the
immediately preceding snapshot. `oracle_actual_CDC_reuse_bytes` requires a
matching source chunk boundary; `target_partition_oracle_reuse_bytes` ignores
source boundaries. Both use the same `oracle_eligible_changed_same_path_target_bytes`
denominator. Their difference is the measured source-boundary loss. Unchanged
files and new paths are reported separately and do not enter this comparison.
Files larger than `oracle-max-file` are reported as skipped and excluded from
the denominator. The oracle uses exact byte comparisons and has no
hash-collision assumption.

Eligible source content is kept in a temporary disk spool, not retained as a
second in-memory snapshot. Memory is therefore bounded mainly by one source
file, one target file, and the suffix array. The spool is removed when the
command exits; use `-oracle-temp-dir` to place it on a filesystem with enough
space for the analyzed snapshots.

Do not compare the global-history `earlier_snapshot_chunk_reuse_bytes` metric
with this adjacent, same-path oracle; they answer different questions.

This is **not** a global “theoretical best” dedup ratio. It excludes matches
against other paths and non-adjacent snapshots, and it keeps the target's
FastCDC boundaries fixed. It also ignores chunk metadata costs.
