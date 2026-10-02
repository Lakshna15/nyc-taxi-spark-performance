"""Data cleaning rules and derived columns for yellow taxi trips."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Longest trip we treat as real. NYC to the far end of Long Island is ~120 road miles, but
# yellow cabs almost never leave the metro area, so 100 miles is generous. The raw data
# contains meter glitches of 100,000+ miles that would wreck any average.
MAX_TRIP_DISTANCE_MILES = 100.0
# The meter starts at $3.00 (TLC "initial charge"), so a lower fare is a data error. Without
# this, $0.01 fares with real tips produce tip percentages of over 1,000,000%.
MIN_FARE_DOLLARS = 3.00
MAX_TRIP_DURATION_HOURS = 6

PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"


def cleaning_rules(year: int) -> list[tuple[str, Column]]:
    """Named conditions a trip must satisfy to be KEPT, applied in this order.

    Built inside a function (not at module level) because Column expressions need an
    active SparkSession to be created.
    """
    pickup, dropoff = F.col(PICKUP), F.col(DROPOFF)
    # The TLC timestamps are timestamp_ntz ("wall-clock" time with no time zone). Build the
    # boundaries from strings: F.lit(datetime(...)) would create a time-zone-aware value
    # from the laptop's local zone and shift midnight by several hours.
    year_start = F.lit(f"{year}-01-01 00:00:00").cast("timestamp_ntz")
    next_year_start = F.lit(f"{year + 1}-01-01 00:00:00").cast("timestamp_ntz")
    max_duration = F.expr(f"INTERVAL {MAX_TRIP_DURATION_HOURS} HOURS")

    return [
        (
            "fare_at_least_minimum",
            (F.col("fare_amount") >= MIN_FARE_DOLLARS) & (F.col("total_amount") > 0),
        ),
        (
            "realistic_distance",
            (F.col("trip_distance") > 0) & (F.col("trip_distance") <= MAX_TRIP_DISTANCE_MILES),
        ),
        # Strictly after: a zero-length trip with a positive distance is a meter error.
        ("dropoff_after_pickup", dropoff > pickup),
        ("duration_at_most_6h", dropoff <= pickup + max_duration),
        (
            "location_ids_present",
            F.col("PULocationID").isNotNull() & F.col("DOLocationID").isNotNull(),
        ),
        # A range check instead of year(pickup) == year: comparing the raw column lets Spark
        # push the filter down to the Parquet reader and skip data using file statistics.
        ("pickup_in_year", (pickup >= year_start) & (pickup < next_year_start)),
    ]


def _keep(condition: Column) -> Column:
    # In SQL, comparing NULL gives NULL, and filter() drops rows where the condition is NULL.
    # Making that explicit (NULL -> False) keeps the row counts in cleaning_report() in sync
    # with what clean_trips() actually removes.
    return F.coalesce(condition, F.lit(False))


def clean_trips(trips: DataFrame, year: int) -> DataFrame:
    """Return only the trips that pass every cleaning rule. Lazy: nothing runs yet."""
    for _, condition in cleaning_rules(year):
        trips = trips.filter(_keep(condition))
    return trips


def cleaning_report(trips: DataFrame, year: int) -> dict[str, int]:
    """Count how many rows each rule removes, in rule order, using ONE pass over the data.

    A row is charged to the first rule it fails, so the counts add up to the total removed.
    The naive approach (filter, count, filter, count, ...) would scan 40M rows once per rule.
    Instead we build one aggregation with a counter column per rule.

    Returns {"input_rows": n, <rule name>: removed, ..., "output_rows": n}.
    """
    passed_earlier_rules = F.lit(True)
    counters = [F.count(F.lit(1)).alias("input_rows")]
    for name, condition in cleaning_rules(year):
        keep = _keep(condition)
        fails_here = passed_earlier_rules & ~keep
        counters.append(F.sum(F.when(fails_here, 1).otherwise(0)).alias(name))
        passed_earlier_rules = passed_earlier_rules & keep

    row = trips.agg(*counters).first()
    # sum() over an empty DataFrame returns NULL, hence the "or 0".
    report = {name: int(value or 0) for name, value in row.asDict().items()}
    report["output_rows"] = report["input_rows"] - sum(
        v for k, v in report.items() if k != "input_rows"
    )
    return report


def add_derived_columns(trips: DataFrame) -> DataFrame:
    """Add analysis columns. Assumes the data is already cleaned (fare > 0, valid times)."""
    duration_seconds = F.unix_timestamp(DROPOFF) - F.unix_timestamp(PICKUP)
    return (
        trips.withColumn("trip_duration_minutes", duration_seconds / 60)
        .withColumn("pickup_hour", F.hour(PICKUP))
        # Spark convention: 1 = Sunday, 2 = Monday, ..., 7 = Saturday
        .withColumn("pickup_day_of_week", F.dayofweek(PICKUP))
        # Tip as a share of the fare (the pre-tip price). Note: cash tips are not recorded
        # in this dataset, so cash trips will show ~0%.
        .withColumn("tip_percentage", F.col("tip_amount") / F.col("fare_amount") * 100)
    )
