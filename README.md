# Parquet Layout Benchmark Engine (DuckDB + PyArrow)

Measures how Parquet file layout (file size, row group size, partitioning, sort order) affects DuckDB query performance, and what a compaction job costs.

## How to run

```bash
make layouts NUM_ROWS=100000   # generate data + build all layouts
make queries                   # compaction + benchmark -> results/*.csv
make test                      # pytest checks (10,000-row run)
```

A `Dockerfile` and `docker-compose.yml` are included (`docker compose run --rm bench bash -c "make layouts && make queries"`).
**Honest note:** Docker Desktop on my machine failed to start (WSL error 0xc00000fd) on the final day, so the Docker setup was validated earlier (tests passed in the container) but the final results were produced with the same Makefile inside Ubuntu on WSL2.

## Environment and scale

- Hardware: <<CPU model>>, 8 GB RAM, Windows 10, Ubuntu (WSL2)
- Versions: duckdb <<x.y.z>>, pyarrow <<x.y.z>>, numpy <<x.y.z>>, Python 3.12 (from `pip list`)
- Seed: 42
- **Scale: 100,000 rows** over 90 days (not 200M). Reduced because of the 8 GB machine and the deadline; the task allows scaling down when hardware is recorded. Relative comparisons between layouts remain valid, absolute numbers do not transfer to large scale.
- DuckDB was limited to `threads=2` and `memory_limit=2GB` for every query on every layout (same limits, so comparisons are fair).
- Layouts with more than 20,000 files (`tiny_files`, `partitioned_by_user_id`) use **2 cold + 2 warm** runs instead of 5 + 5, because each cold run takes minutes. Their variance is therefore less well characterised.

## Requirement map

| Requirement | Artifact |
|---|---|
| req-data-generation | `src/generate_data.py`, `data/raw/events.parquet` |
| req-layout-building | `src/build_layouts.py`, `data/layouts/` |
| req-query-profiling | `src/run_queries.py`, `results/layout.csv`, `results/profiles/` |
| req-compaction-job | `src/compact.py`, `results/compaction.csv` |
| req-docker-compose-setup | `Dockerfile`, `docker-compose.yml`, `Makefile` |
| req-env-example | `.env.example` |
| req-readme-analysis | this file |
| req-dependencies | `requirements.txt` |
| req-high-cardinality-failure | `results/failure_user_partition.csv` |
| req-schema-checksum | integrity gate in `src/run_queries.py`, checksum in `src/compact.py` |

## Layouts

`tiny_files` (1 file/minute), `hourly`, `daily`, `daily_rg_10k`, `daily_rg_1m`, `partitioned_by_date`, `partitioned_by_date_country`, `sorted_by_user`, `sorted_by_ts`, `partitioned_by_user_id` (induced failure), `compacted` (output of compaction), plus an extra control `daily_rg_small` (unsorted, same row group size as the sorted layouts) so that sorting is isolated from row group size.

At this small scale each daily file has only ~1,100 rows, so the sorted layouts and `daily_rg_small` use a reduced row group size (`row_group_rows` column in `layout.csv`, <<value>>) so that several row groups exist per file and pruning is observable.

## Results

All numbers below come from `results/layout.csv` via `python scripts/analyze.py`.

### 1. tiny_files vs daily (one-day range query)

| Cache | tiny_files median ms | daily median ms | Latency ratio |
|---|---|---|---|
| cold | <<>> | <<>> | <<>>x |
| warm | <<>> | <<>> | <<>>x |

`tiny_files` has <<N>> files, so DuckDB must open and read a footer for every file before scanning. Peak RSS: tiny_files <<>> MB vs daily <<>> MB.

> "Even small Parquet files can add up fast, especially once you account for all the metadata and row groups that get loaded into memory." (Olga Braginskaya)

This shows up in the `files`, `latency_ms` and `peak_rss_mb` columns for `tiny_files` vs `daily`.

### 2. Row groups pruned: user_id point lookup

| Layout | Row groups total | Pruned | % pruned |
|---|---|---|---|
| sorted_by_user | <<>> | <<>> | <<>>% |
| daily (unsorted) | <<>> | 0 | 0% |
| daily_rg_small (unsorted, same row group size) | <<>> | <<>> | <<>>% |

The `daily_rg_small` row shows that sorting, not row group size, causes the pruning.

> "Parquet's predicate pushdown is only as useful as your data is sorted." (Alexander Rocha)

### 3. Row group size (10k vs 122,880 vs 1M, warm median ms)

| Query | 10k | 122,880 (default) | 1M | Fastest |
|---|---|---|---|---|
| point_lookup | <<>> | <<>> | <<>> | <<>> |
| range_1day | <<>> | <<>> | <<>> | <<>> |
| country_filter | <<>> | <<>> | <<>> | <<>> |
| full_agg | <<>> | <<>> | <<>> | <<>> |
| topk_week | <<>> | <<>> | <<>> | <<>> |

**Limitation:** at 100,000 rows each daily file holds ~1,100 rows, so 10k, 122,880 and 1M row group sizes all produce a single row group per file and the three layouts are nearly identical. Differences here are noise, not a real row group effect. A meaningful comparison needs a much larger dataset.

**"Bigger files are always better" does not hold:** <<name the query/layout where a larger or fewer-file layout was slower or pruned nothing, with numbers; for example the point lookup on unsorted daily files prunes 0 row groups>>.

**Recommendation:** write daily (or hourly) files, sort by the most selective filter column (`user_id` for lookups), and use row groups small enough that sorting gives pruning granularity, rather than one huge row group per file. Never write per-minute files without a compaction job.

### 4. Compaction cost (`results/compaction.csv`)

| Input files | Output files | Wall s | Peak RSS MB | Rows before = after | checksum_match |
|---|---|---|---|---|---|
| <<>> | <<>> | <<>> | <<>> | <<>> | true |

Compaction processes one day at a time to keep memory bounded, sorts by `user_id`, and verifies row count and `SUM(value)` (computed in integer cents to avoid float error) before and after. Query latency before vs after compaction (`tiny_files` vs `compacted`, range_1day warm): <<>> ms vs <<>> ms.

### 5. Induced failure: partition by user_id (`results/failure_user_partition.csv`)

<<files_written>> files were created for <<distinct_users>> distinct users (<<total_bytes>> bytes, partial=<<True/False>>, build time <<>> s). Each file holds only a few rows. Query latency on this layout vs `daily`: <<>> ms vs <<>> ms (range_1day, warm). High-cardinality partitioning explodes file count and metadata overhead; the script caps the number of partitions (`MAX_USER_PARTITIONS`) and catches `OSError` so the pipeline continues.

### 6. Metadata evidence (`parquet_metadata`, user_id)

sorted_by_user (file `<<name>>`):
```
<<paste output>>
```
The min/max ranges per row group are narrow and non-overlapping, so a lookup for one user_id touches one row group and the rest are skipped.

daily (file `<<name>>`):
```
<<paste output>>
```
Every row group's min/max spans almost the whole user_id range, so no row group can be skipped.

## Data integrity

`run_queries.py` computes `SUM(CAST(ROUND(value*100) AS BIGINT))` for every layout and compares it with the raw file. A mismatch exits with an error, so `make queries` fails. The partial `partitioned_by_user_id` layout is skipped if it was capped.

## Methodology and caveats

- **Cold** = a fresh Python process and fresh DuckDB connection. The **OS page cache is not cleared**, so cold runs are faster than true cold and the cold/warm gap is understated.
- `row_groups_total` and `row_groups_pruned` are computed from parquet footer min/max (`parquet_metadata()`) for the filter column, not read from the DuckDB profile (key names vary by version). Raw profile nodes are saved in `results/profiles/`.
- `bytes_read` is the process `read_chars` counter delta (bytes requested from the OS, may be served from cache).
- `peak_rss_mb` is `ru_maxrss` of the per-run process (KB on Linux, converted to MB).
- The hive-partitioned layouts are not filtered on `date`/`country` by the fixed queries, except the `country = 'US'` query on `partitioned_by_date_country`.
- Results come from a single machine at 100,000 rows; treat them as relative evidence, not absolute benchmarks.
- Package versions differ slightly from `requirements.txt` pins because pandas pulled a newer numpy during repair of the environment.