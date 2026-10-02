"""The performance experiments run by src/benchmark.py.

Each experiment runs the same workload in a "baseline" and an "optimized" variant (the
shuffle-partitions experiment adds a third, "aqe_on"). Besides timings, each one saves
"notes" to docs/plans/<name>.txt: the query plans and Spark UI metrics that explain
*why* one variant is faster.
"""

import contextlib
import io
import json
import logging
import time
import urllib.request
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.clean import PICKUP, add_derived_columns, clean_trips
from src.ingest import read_trips, read_zones
from src.transform import build_output_tables, join_zones

log = logging.getLogger(__name__)

YEAR = config.YEAR


@dataclass
class Variant:
    name: str
    workload: Callable[[SparkSession], None]  # must run at least one action
    conf: dict[str, str] = field(default_factory=dict)  # runtime settings for this variant only


@dataclass
class Experiment:
    name: str
    description: str
    variants: list[Variant]
    prepare: Callable[[SparkSession], None] | None = None  # untimed setup, e.g. writing a copy
    notes: Callable[[SparkSession], str] | None = None  # plans/metrics saved to docs/plans/


# --- Helpers ---------------------------------------------------------------------------


@contextmanager
def spark_conf(spark: SparkSession, settings: dict[str, str]) -> Iterator[None]:
    """Temporarily apply runtime SQL settings, restoring the previous values afterwards."""
    previous = {key: spark.conf.get(key, None) for key in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, old in previous.items():
            if old is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, old)


def run_to_noop(df: DataFrame) -> None:
    """Execute the whole query but discard the result.

    The "noop" sink makes Spark compute every row without writing files, so timings measure
    the query itself rather than disk writes. (count() would be a poor benchmark action:
    Spark can skip work, e.g. columns it doesn't need, to produce a count.)
    """
    df.write.format("noop").mode("overwrite").save()


def explain_string(df: DataFrame, mode: str = "formatted") -> str:
    """df.explain() prints the plan; capture it as text instead."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode=mode)
    return buffer.getvalue()


def _ui_api(spark: SparkSession, path: str):
    """GET from the Spark UI's REST API (served on port 4040 while the session is alive)."""
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.load(response)


def last_scan_metrics(spark: SparkSession) -> list[dict[str, str]]:
    """Metrics of the file-scan nodes in the most recent query (files read, bytes read, ...)."""
    time.sleep(2)  # the UI records query metrics asynchronously, just after the query ends
    queries = _ui_api(spark, "sql?details=true&length=10000")
    with_scans = [q for q in queries if any(n["nodeName"].startswith("Scan") for n in q["nodes"])]
    latest = max(with_scans, key=lambda q: q["id"])
    return [
        {"node": node["nodeName"], **{m["name"]: m["value"] for m in node.get("metrics", [])}}
        for node in latest["nodes"]
        if node["nodeName"].startswith("Scan")
    ]


def stage_task_counts(spark: SparkSession, description: str) -> list[str]:
    """One line per completed stage of the jobs labelled with `description`."""
    time.sleep(2)
    stages = _ui_api(spark, "stages?status=complete")
    ours = sorted(
        (s for s in stages if s.get("description") == description), key=lambda s: s["stageId"]
    )
    return [f"stage {s['stageId']}: {s['numTasks']:>4} tasks  ({s['name']})" for s in ours]


def _parquet_bytes(path: Path) -> int:
    """Size of the data files under a directory (ignores Spark's _SUCCESS and .crc files)."""
    return sum(
        p.stat().st_size for p in path.rglob("*") if p.is_file() and p.name.startswith("part-")
    )


def _format_metrics(metrics: list[dict[str, str]]) -> str:
    return "\n".join("  " + ", ".join(f"{k}: {v}" for k, v in m.items()) for m in metrics)


def _cleaned_trips(spark: SparkSession) -> DataFrame:
    return add_derived_columns(clean_trips(read_trips(spark, YEAR), YEAR))


# --- 1. File format: CSV vs Parquet ------------------------------------------------------

CSV_DIR = config.BENCH_DIR / "trips_csv"


def _revenue_by_pickup_zone(trips: DataFrame) -> DataFrame:
    return trips.groupBy("PULocationID").agg(
        F.sum("total_amount").alias("revenue"), F.count(F.lit(1)).alias("trip_count")
    )


def _read_csv_copy(spark: SparkSession) -> DataFrame:
    # Give the CSV reader the same schema as the Parquet files, so we compare file formats,
    # not schema inference (inferSchema would add another full pass over the CSV).
    schema = read_trips(spark, YEAR).schema
    return spark.read.csv(CSV_DIR.as_posix(), header=True, schema=schema)


def _write_csv_copy(spark: SparkSession) -> None:
    if (CSV_DIR / "_SUCCESS").exists():
        log.info("CSV copy already exists, skipping")
        return
    log.info("Writing a CSV copy of the raw data (one-time, untimed)...")
    read_trips(spark, YEAR).write.mode("overwrite").option("header", True).csv(CSV_DIR.as_posix())


def _file_format_notes(spark: SparkSession) -> str:
    raw_files = [config.trip_file(YEAR, m) for m in range(1, 13)]
    parquet_bytes = sum(p.stat().st_size for p in raw_files)
    csv_bytes = _parquet_bytes(CSV_DIR)
    csv_df, parquet_df = _read_csv_copy(spark), read_trips(spark, YEAR)

    lines = [
        f"Size on disk: CSV {csv_bytes / 1e9:.2f} GB vs Parquet {parquet_bytes / 1e9:.2f} GB",
        f"Row count check: CSV {csv_df.count():,} vs Parquet {parquet_df.count():,}",
    ]
    for label, df in (("CSV", csv_df), ("Parquet", parquet_df)):
        query = _revenue_by_pickup_zone(df)
        run_to_noop(query)
        lines += [
            f"\n=== {label}: physical plan ===",
            explain_string(query),
            f"{label}: scan metrics from the Spark UI",
            _format_metrics(last_scan_metrics(spark)),
        ]
    return "\n".join(lines)


def file_format_experiment() -> Experiment:
    return Experiment(
        name="file_format",
        description="Read 12 months + aggregate revenue by pickup zone: CSV vs Parquet",
        prepare=_write_csv_copy,
        notes=_file_format_notes,
        variants=[
            Variant("baseline", lambda s: run_to_noop(_revenue_by_pickup_zone(_read_csv_copy(s)))),
            Variant(
                "optimized",
                lambda s: run_to_noop(_revenue_by_pickup_zone(read_trips(s, YEAR))),
            ),
        ],
    )


# --- 2. Partition pruning ----------------------------------------------------------------

PARTITIONED_DIR = config.BENCH_DIR / "trips_partitioned"
UNPARTITIONED_DIR = config.BENCH_DIR / "trips_unpartitioned"
PRUNE_MONTH = 6


def _trips_with_year_month(spark: SparkSession) -> DataFrame:
    return (
        _cleaned_trips(spark)
        .withColumn("pickup_year", F.year(PICKUP))
        .withColumn("pickup_month", F.month(PICKUP))
    )


def _write_layouts(spark: SparkSession) -> None:
    """Write the same cleaned data twice: partitioned by year/month, and as one flat folder."""
    if not (PARTITIONED_DIR / "_SUCCESS").exists():
        log.info("Writing year/month-partitioned copy (one-time, untimed)...")
        (
            _trips_with_year_month(spark)
            .write.mode("overwrite")
            .partitionBy("pickup_year", "pickup_month")
            .parquet(PARTITIONED_DIR.as_posix())
        )
    if not (UNPARTITIONED_DIR / "_SUCCESS").exists():
        log.info("Writing unpartitioned copy (one-time, untimed)...")
        _trips_with_year_month(spark).write.mode("overwrite").parquet(UNPARTITIONED_DIR.as_posix())


def _one_month_by_weekday(spark: SparkSession, path: Path) -> DataFrame:
    trips = spark.read.parquet(path.as_posix())
    return (
        trips.filter((F.col("pickup_year") == YEAR) & (F.col("pickup_month") == PRUNE_MONTH))
        .groupBy("pickup_day_of_week")
        .agg(F.count(F.lit(1)).alias("trip_count"), F.sum("total_amount").alias("revenue"))
    )


def _partition_pruning_notes(spark: SparkSession) -> str:
    def data_files(path: Path) -> int:
        return sum(1 for p in path.rglob("part-*.parquet"))

    month_dir = PARTITIONED_DIR / f"pickup_year={YEAR}" / f"pickup_month={PRUNE_MONTH}"
    lines = [
        f"Unpartitioned: {data_files(UNPARTITIONED_DIR)} data files in one folder",
        f"Partitioned:   {data_files(PARTITIONED_DIR)} data files in "
        f"{sum(1 for _ in PARTITIONED_DIR.glob('pickup_year=*/pickup_month=*'))} year/month "
        f"folders, {data_files(month_dir)} of them in {month_dir.relative_to(config.BENCH_DIR)}",
    ]
    for label, path in (("Unpartitioned", UNPARTITIONED_DIR), ("Partitioned", PARTITIONED_DIR)):
        query = _one_month_by_weekday(spark, path)
        run_to_noop(query)
        lines += [
            f"\n=== {label}: physical plan (look at PartitionFilters vs PushedFilters) ===",
            explain_string(query),
            f"{label}: scan metrics from the Spark UI",
            _format_metrics(last_scan_metrics(spark)),
        ]
    return "\n".join(lines)


def partition_pruning_experiment() -> Experiment:
    return Experiment(
        name="partition_pruning",
        description=f"Query one month (month {PRUNE_MONTH}): unpartitioned vs year/month folders",
        prepare=_write_layouts,
        notes=_partition_pruning_notes,
        variants=[
            Variant("baseline", lambda s: run_to_noop(_one_month_by_weekday(s, UNPARTITIONED_DIR))),
            Variant("optimized", lambda s: run_to_noop(_one_month_by_weekday(s, PARTITIONED_DIR))),
        ],
    )


# --- 3. Broadcast join -------------------------------------------------------------------

# Turn off automatic broadcasting so the baseline has to use a sort-merge join.
NO_AUTO_BROADCAST = {"spark.sql.autoBroadcastJoinThreshold": "-1"}


def _borough_flows(spark: SparkSession, broadcast_zones: bool) -> DataFrame:
    joined = join_zones(_cleaned_trips(spark), read_zones(spark), broadcast_zones=broadcast_zones)
    return joined.groupBy("pickup_borough", "dropoff_borough").count()


def _broadcast_join_notes(spark: SparkSession) -> str:
    lines = []
    with spark_conf(spark, NO_AUTO_BROADCAST):
        for label, hint in (("baseline (no hint)", False), ("optimized (broadcast hint)", True)):
            lines += [
                f"\n=== {label}: physical plan ===",
                explain_string(_borough_flows(spark, hint)),
            ]
    return "\n".join(lines)


def broadcast_join_experiment() -> Experiment:
    return Experiment(
        name="broadcast_join",
        description="Join trips to zones twice: sort-merge join vs broadcast() hint",
        notes=_broadcast_join_notes,
        variants=[
            Variant("baseline", lambda s: run_to_noop(_borough_flows(s, False)), NO_AUTO_BROADCAST),
            Variant("optimized", lambda s: run_to_noop(_borough_flows(s, True)), NO_AUTO_BROADCAST),
        ],
    )


# --- 4. Shuffle partitions and AQE -------------------------------------------------------


def _trip_segments(spark: SparkSession) -> DataFrame:
    """A shuffle-heavy aggregation: millions of (date, hour, pickup, dropoff) groups."""
    return (
        _cleaned_trips(spark)
        .groupBy(
            F.to_date(PICKUP).alias("pickup_date"), "pickup_hour", "PULocationID", "DOLocationID"
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
            F.sum("total_amount").alias("revenue"),
        )
    )


def _shuffle_variants(spark: SparkSession) -> list[Variant]:
    # On one machine, one task per core per wave is a sensible starting point.
    tuned = str(spark.sparkContext.defaultParallelism)

    def settings(partitions: str, aqe: bool) -> dict[str, str]:
        return {
            "spark.sql.shuffle.partitions": partitions,
            "spark.sql.adaptive.enabled": str(aqe).lower(),
        }

    def workload(s: SparkSession) -> None:
        run_to_noop(_trip_segments(s))

    return [
        Variant("baseline", workload, settings("200", aqe=False)),
        Variant("optimized", workload, settings(tuned, aqe=False)),
        Variant("aqe_on", workload, settings("200", aqe=True)),
    ]


def _shuffle_notes(spark: SparkSession) -> str:
    lines = []
    for variant in _shuffle_variants(spark):
        label = f"notes | shuffle_partitions | {variant.name}"
        spark.sparkContext.setJobDescription(label)
        with spark_conf(spark, variant.conf):
            variant.workload(spark)
        spark.sparkContext.setJobDescription(None)
        lines += [f"\n=== {variant.name}: {variant.conf} ===", *stage_task_counts(spark, label)]
    return "\n".join(lines)


def shuffle_partitions_experiment(spark: SparkSession) -> Experiment:
    return Experiment(
        name="shuffle_partitions",
        description="Shuffle-heavy groupBy: 200 vs tuned shuffle partitions (AQE off), then AQE on",
        notes=_shuffle_notes,
        variants=_shuffle_variants(spark),
    )


# --- 5. Caching --------------------------------------------------------------------------

# Only the columns the output tables use: caching fewer columns uses less memory.
CACHE_COLUMNS = [
    "tpep_pickup_datetime",
    "PULocationID",
    "DOLocationID",
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
    "payment_type",
    "total_amount",
    "tip_percentage",
    "trip_duration_minutes",
    "pickup_hour",
    "pickup_day_of_week",
]


def _enriched_trips(spark: SparkSession) -> DataFrame:
    trips = join_zones(_cleaned_trips(spark), read_zones(spark), broadcast_zones=True)
    return trips.select(*CACHE_COLUMNS)


def _all_output_tables(trips: DataFrame) -> None:
    """The pipeline's four aggregations: four separate actions on the same input."""
    for table in build_output_tables(trips).values():
        run_to_noop(table)


def _with_cache(spark: SparkSession) -> None:
    trips = _enriched_trips(spark).cache()  # lazy: filled in during the first action
    _all_output_tables(trips)
    trips.unpersist()


def _caching_notes(spark: SparkSession) -> str:
    trips = _enriched_trips(spark).cache()
    _all_output_tables(trips)
    time.sleep(2)
    cached = _ui_api(spark, "storage/rdd")
    plan = explain_string(build_output_tables(trips)["tip_by_payment_type"])
    trips.unpersist()

    lines = ["Cached data (from the Spark UI Storage page):"]
    for rdd in cached:
        lines.append(
            f"  {rdd['numCachedPartitions']}/{rdd['numPartitions']} partitions cached, "
            f"memory {rdd['memoryUsed'] / 1e9:.2f} GB, disk {rdd['diskUsed'] / 1e9:.2f} GB"
        )
    lines += ["\n=== Plan of one output table reading from the cache (InMemoryTableScan) ===", plan]
    return "\n".join(lines)


def caching_experiment() -> Experiment:
    return Experiment(
        name="caching",
        description="Four aggregations on the same enriched trips: no cache vs .cache()",
        notes=_caching_notes,
        variants=[
            Variant("baseline", lambda s: _all_output_tables(_enriched_trips(s))),
            Variant("optimized", _with_cache),
        ],
    )


def build_experiments(spark: SparkSession) -> list[Experiment]:
    return [
        file_format_experiment(),
        partition_pruning_experiment(),
        broadcast_join_experiment(),
        shuffle_partitions_experiment(spark),
        caching_experiment(),
    ]
