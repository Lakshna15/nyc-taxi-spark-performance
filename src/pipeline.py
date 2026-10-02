"""Run the pipeline end to end: ingest -> clean -> join zones -> aggregate -> write Parquet."""

import logging
import time
from pathlib import Path

from pyspark.sql import DataFrame

from src import config
from src.clean import add_derived_columns, clean_trips, cleaning_report
from src.ingest import read_trips, read_zones
from src.spark_session import build_spark
from src.transform import build_output_tables, join_zones

log = logging.getLogger(__name__)


def log_cleaning_report(report: dict[str, int]) -> None:
    total = report["input_rows"]
    log.info("Rows read: %s", f"{total:,}")
    for rule, removed in report.items():
        if rule in ("input_rows", "output_rows"):
            continue
        log.info("  %-25s removed %12s (%5.2f%%)", rule, f"{removed:,}", 100 * removed / total)
    kept = report["output_rows"]
    log.info("Rows kept: %s (%.2f%%)", f"{kept:,}", 100 * kept / total)


def write_table(df: DataFrame, path: Path) -> None:
    # Each partition becomes one output file. These results are small (at most a few
    # thousand rows), so coalesce(1) writes a single file instead of many tiny ones.
    df.coalesce(1).write.mode("overwrite").parquet(path.as_posix())


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)  # py4j logs every JVM call at INFO

    spark = build_spark()
    try:
        raw = read_trips(spark, config.YEAR)

        start = time.perf_counter()
        report = cleaning_report(raw, config.YEAR)  # action: one full pass over the data
        log.info("Cleaning report computed in %.1fs", time.perf_counter() - start)
        log_cleaning_report(report)

        trips = add_derived_columns(clean_trips(raw, config.YEAR))
        enriched = join_zones(trips, read_zones(spark))

        # Note: each write below is a separate action, so Spark re-reads and re-cleans the
        # raw data for every table. Phase 5's caching experiment measures what that costs.
        for name, table in build_output_tables(enriched).items():
            start = time.perf_counter()
            write_table(table, config.OUTPUT_DIR / name)
            log.info("Wrote %-26s in %5.1fs", name, time.perf_counter() - start)

        # Read a couple of the (small) outputs back as a quick sanity check.
        for name in ("tip_by_payment_type", "top_zone_pairs"):
            log.info("Preview of %s:", name)
            spark.read.parquet((config.OUTPUT_DIR / name).as_posix()).show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
