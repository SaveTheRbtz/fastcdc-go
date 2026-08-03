# fastcdc-lab

`fastcdc-lab` contains reproducible experiments for the FastCDC 2020
implementation. It is separate from the end-user `fastcdc` command. Every
subcommand writes CSV to standard output; redirect it to keep a result.

Sizes accept bytes or `KiB`, `MiB`, and `GiB` suffixes. The default chunk
format is average 8 KiB, minimum 2 KiB, maximum 32 KiB, normalization level 1.
Every subcommand also accepts `-average`, `-min`, `-max`, and
`-normalization`.

## Chunk-size distribution

Write observed histogram bins and the independent-uniform-hash model:

```sh
go run -race ./cmd/fastcdc-lab distribution \
  -bytes 1GiB -seed 1 \
  > results/distribution.csv
```

The final EOF-shortened chunk is excluded. A final chunk exactly `MaxSize`
bytes long is retained because it is a complete forced chunk. The analytical
columns describe the two-hazard model implied by the small and large masks;
they do not assume that successive Gear hashes are actually independent.

Plotting is optional and stays outside the Go command:

```sh
gnuplot -c cmd/fastcdc-lab/distribution.gnuplot \
  results/distribution.csv results/distribution.svg
```

## Deduplication

Analyze directories in positional order:

```sh
go run -race ./cmd/fastcdc-lab dedup snapshot-v1 snapshot-v2 \
  > results/dedup.csv
```

Analyze Git revisions without checking them out:

```sh
go run -race ./cmd/fastcdc-lab dedup \
  -repo /path/to/linux \
  -rev v7.0 -rev v7.1 \
  > results/linux-dedup.csv
```

Git trees are read with `git ls-tree` and one `git cat-file --batch` process.
Only regular blobs are included. Directory symlinks and special files are
skipped. Each regular file is chunked independently, so boundaries cannot
cross file edges.

The CSV keeps these values separate:

- `earlier_snapshot_chunk_reuse_bytes` counts target bytes in chunks seen in
  any earlier snapshot. Same-snapshot repetitions do not inflate it.
- `unique_chunk_bytes` is the payload needed to store one copy of each chunk
  across all snapshots.
- `earlier_snapshot_exact_file_reuse_bytes` and
  `unique_exact_file_bytes` provide a whole-file baseline.
- `changed_same_path_cdc_reuse_bytes` excludes unchanged files, which would
  otherwise hide boundary behavior on edited content.

Chunk and file identities use SHA-256 plus length. Aggregate dedup metrics do
not byte-compare hash collisions. Payload savings exclude recipes, indexes,
metadata, and compression.

## Target-partition oracle

For exactly two snapshots, `-oracle` compares normal CDC reuse with an exact
same-path test that keeps the target partition but ignores source boundaries:

```sh
go run -race ./cmd/fastcdc-lab dedup \
  -repo /path/to/linux -rev v7.0 -rev v7.1 \
  -oracle -oracle-max-file 1MiB \
  > results/linux-oracle.csv
```

Eligible source files are held in a temporary disk spool. `os.CreateTemp`
uses the standard `TMPDIR` setting when a different filesystem is needed. The
spool is removed on exit and excluded if it lies inside a directory snapshot.

`oracle_actual_cdc_reuse_bytes` requires matching source chunk boundaries.
`target_partition_oracle_reuse_bytes` asks whether each exact target chunk
occurs anywhere in the preceding same-path file. Their difference is source
boundary loss. Files above `oracle-max-file` are reported as skipped.

This oracle is not a global theoretical optimum: it keeps target boundaries,
does not search other paths, and covers only the adjacent pair. Normal dedup
memory grows with unique chunks and the preceding snapshot's per-file chunk
sets; the oracle additionally uses a suffix array for one eligible file at a
time.
