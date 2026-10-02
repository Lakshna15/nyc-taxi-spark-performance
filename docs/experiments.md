# Experiment notes

All numbers below come from `results/benchmark_results.csv` (run of 2026-10-02, median of 3
runs per variant). The raw evidence (full query plans and Spark UI metrics) for each
experiment is in [`docs/plans/`](plans/), written automatically by `src/benchmark.py`.

**Machine:** Intel Core Ultra 5 225H (14 cores), Spark 3.5.9 in `local[*]` mode inside Docker
Desktop, whose Linux VM had 14 CPUs and 7.5 GB of RAM (host: 15.4 GB). Spark driver memory 4 GB.

**Data:** NYC TLC yellow taxi trips, all of 2024: 41,169,720 raw rows, 39,585,378 after cleaning.

## How to take Spark UI screenshots

The Spark UI only exists while a Spark application is running. To keep it up after the
experiments finish:

```bash
make docker-benchmark ARGS="--hold-ui broadcast_join"
```

Open <http://localhost:4040>, take the screenshots, then press Enter in the terminal. Every
job is labelled `<experiment> | <variant> | run N/3` (via `setJobDescription`), so use the
**Description** column on the **Jobs** page to find the right one. Save the images as
`docs/screenshots/<experiment>_<what>.png`.

---

## 1. File format: CSV vs Parquet

**Concept:** CSV is plain text in rows: to read even one column, Spark must read and parse
every character of every line. Parquet is *columnar* and compressed: values are stored
column by column, with min/max statistics, so Spark reads only the columns a query needs.

**Workload:** read all 12 months and compute revenue and trip count per pickup zone. The CSV
reader gets an explicit schema, so this compares formats, not schema guessing.

| | Size on disk | Median time |
|---|---:|---:|
| CSV (baseline) | 4.47 GB | 5.71 s |
| Parquet (optimized) | 0.69 GB | 0.47 s |

**12.15x faster.** Both scans produce the same 41,169,720 rows. Both plans show
`ReadSchema: struct<PULocationID:int,total_amount:double>`, but for CSV that's applied only
*after* parsing each full line. The CSV scan read 4.2 GiB; Parquet read just those two
columns from the 12 files.

**Screenshots:**
- **SQL / DataFrame** tab → open a `file_format | baseline` query and a
  `file_format | optimized` query. Show the `Scan csv` and `Scan parquet` boxes with their
  "size of files read" metrics.
- **Stages** tab → compare the *Input* column (bytes read) for the two scan stages.

## 2. Partition pruning

**Concept:** writing data with `partitionBy("pickup_year", "pickup_month")` creates one folder
per month (`pickup_year=2024/pickup_month=6/`). A filter on those columns lets Spark skip
whole folders without opening them. That's *partition pruning*.

**Workload:** trips per day of week for June only, reading either the flat copy (14 files)
or the partitioned copy (42 files in 12 month folders).

| | Files read | Size of files read | Scan time | Median time |
|---|---:|---:|---:|---:|
| Unpartitioned (baseline) | 14 | 908.6 MiB | 701 ms | 0.32 s |
| Partitioned (optimized) | 4 | 80.6 MiB | 209 ms | 0.25 s |

**1.28x faster.** The plan proves the pruning: the partitioned scan has
`PartitionFilters: [... (pickup_month = 6)]` and "number of partitions read: 1". The
unpartitioned scan can only use `PushedFilters` (Parquet row-group statistics), so it must
open the footer of all 14 files.

**Why the time gain is small:** one month is only ~3.4M rows, so the whole query takes a
third of a second and fixed costs (planning, starting tasks) dominate. Spark still read **11x
fewer bytes**. That gap grows with data size: with 10 years of data, the partitioned query
would still open one folder, while the flat table would open every file.

**Screenshots:**
- **SQL / DataFrame** tab → each `partition_pruning` query's DAG, zoomed on the
  `Scan parquet` box: "number of files read" 14 vs 4 and "number of partitions read: 1".
- Paste the two `== Physical Plan ==` scan sections from `docs/plans/partition_pruning.txt`
  next to them.

## 3. Broadcast join

**Concept:** to join two tables, matching keys must meet on the same core. A **sort-merge
join** shuffles *both* sides by the join key and sorts them: here, all 39.6M trips, twice
(pickup and dropoff zone). A **broadcast hash join** instead copies the tiny table (265 zones)
to every task, so the big table never moves.

**Workload:** join cleaned trips to zones for pickup and dropoff, then count trips per
borough pair. `spark.sql.autoBroadcastJoinThreshold = -1` in both variants turns off
automatic broadcasting; the optimized variant adds an explicit `broadcast()` hint.

| | Join strategy in plan | Median time |
|---|---|---:|
| Baseline | `SortMergeJoin` (x2), each with `Exchange hashpartitioning` + `Sort` | 8.02 s |
| Optimized | `BroadcastHashJoin` (x2) with `BroadcastExchange` | 2.14 s |

**3.75x faster.** Note: without the `-1` setting, Spark broadcasts the zone table
automatically (it's far below the 10 MB default threshold). The baseline is "what happens
when Spark can't tell the table is small", which is common when the small side is the result
of a complex query.

**Screenshots:**
- **SQL / DataFrame** tab → DAGs of a `broadcast_join | baseline` query (two `SortMergeJoin`
  boxes fed by `Exchange` and `Sort`) and a `broadcast_join | optimized` query
  (`BroadcastHashJoin` fed by `BroadcastExchange`).
- **Stages** tab → the baseline has extra stages with large *Shuffle Write*; the optimized
  version has almost none.

## 4. Shuffle partitions and AQE

**Concept:** after a shuffle, Spark splits the data into `spark.sql.shuffle.partitions`
pieces (default **200**), one task each. Too many → lots of tiny tasks with scheduling
overhead; too few → huge tasks that may run out of memory. **Adaptive Query Execution (AQE)**
looks at the real shuffle size at runtime and merges small partitions automatically.

**Workload:** group trips by (date, hour, pickup zone, dropoff zone), which is millions of
groups, and compute count, average duration and revenue.

| Variant | Settings | Tasks after the shuffle | Median time |
|---|---|---:|---:|
| baseline | 200 partitions, AQE off | 200 | 6.60 s |
| optimized | 14 partitions (= cores), AQE off | 14 | 6.43 s |
| aqe_on | 200 partitions, AQE on | 15 | 6.75 s |

**No meaningful difference (1.03x and 0.98x, within run-to-run noise).** The settings did
what they should: AQE turned 200 planned partitions into 15 at runtime. But the time is
dominated by the 14-task scan-and-clean stage *before* the shuffle. Spark also pre-aggregates
on each core before shuffling, which shrinks the data that has to move. On this machine,
starting 200 short tasks on 14 cores cost only ~0.2 s more than 14 tasks.

**Takeaway:** shuffle tuning matters when the shuffle is large (big joins, wide
aggregations on a cluster) or when 200 is far too *few* (terabytes). On a laptop with a
small shuffle, the default plus AQE (on by default since Spark 3.2) is fine.

**Screenshots:**
- **Stages** tab → the second stage of each variant: 200 vs 14 tasks (AQE off).
- **SQL / DataFrame** tab → an `aqe_on` query: the `AQEShuffleRead` box shows
  "number of partitions: 15" (coalesced from 200).

## 5. Caching

**Concept:** `df.cache()` keeps a DataFrame in memory after the first action computes it, so
later actions reuse it instead of recomputing from the source. Without it, every action
re-runs the whole plan (read → clean → join).

**Workload:** the pipeline's four output aggregations (four actions) on the same cleaned,
zone-enriched trips, reduced to the 13 columns they need.

| | Median time |
|---|---:|
| No cache (baseline) | 16.49 s |
| `.cache()` (optimized) | 21.23 s |

**Caching was 0.78x, so 29% slower.** The cache held 1.77 GB in memory (14/14 partitions,
nothing spilled to disk). The first action had to compute *and* store 39.6M rows × 13
columns in Spark's in-memory format. The baseline instead re-read Parquet four times, and
each time Spark read only the columns needed for cleaning plus that one aggregation.
Re-reading compressed, columnar Parquet from local disk (likely already in the OS file
cache after the first run) turned out cheaper than building a 1.77 GB cache that's used
only three more times.

**When caching helps:** the same DataFrame is reused many times; it's expensive to
recompute (slow source like CSV or a remote database, heavy joins, UDFs); and it fits in
memory. Typical cases are iterative ML, or interactive exploration of one filtered subset.

**When it wastes memory (or time):** it's used once or twice; the source is already fast
and columnar; or it doesn't fit, so it spills to disk and steals memory from shuffles and
joins. Always `unpersist()` when done.

**Screenshots:**
- **Storage** tab while a cached run is active: the cached DataFrame with "Fraction Cached
  100%" and "Size in Memory ~1.8 GB".
- **SQL / DataFrame** tab → a `caching | optimized` query showing `InMemoryTableScan`,
  versus a `caching | baseline` query showing `Scan parquet` → `Filter` → `BroadcastHashJoin`.
