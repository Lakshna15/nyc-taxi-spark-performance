"""Run the pipeline end to end.

Current steps: ingest all 12 months -> clean -> add derived columns.
Phase 3 adds the zone join, aggregations, and writing outputs to data/output/.
"""

import logging
import time

from src import config
from src.clean import add_derived_columns, clean_trips, cleaning_report
from src.ingest import read_trips
from src.spark_session import build_spark

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

        # Sanity-check the derived columns on the real data (another full pass).
        derived = ["trip_duration_minutes", "pickup_hour", "pickup_day_of_week", "tip_percentage"]
        trips.select(derived).summary("min", "50%", "max").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
