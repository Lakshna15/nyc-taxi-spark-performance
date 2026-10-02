"""Joins and aggregations that turn cleaned trips into the output tables."""

from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# From the TLC yellow taxi data dictionary (March 2025 edition).
PAYMENT_TYPE_NAMES = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}


def _zone_columns(zones: DataFrame, prefix: str) -> DataFrame:
    """The zone table with columns renamed for one side of the trip, e.g. pickup_borough."""
    return zones.select(
        F.col("LocationID").alias(f"{prefix}_location_id"),
        F.col("Borough").alias(f"{prefix}_borough"),
        F.col("Zone").alias(f"{prefix}_zone"),
    )


def join_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Attach borough and zone names for both the pickup and dropoff locations.

    We join the same lookup table twice, once per side. A left join keeps every trip
    even if its location ID were missing from the lookup (its names would be null).
    The zone table is tiny (265 rows), so Spark broadcasts it automatically. Phase 5
    measures what happens when it doesn't.
    """
    pickup_zones = _zone_columns(zones, "pickup")
    dropoff_zones = _zone_columns(zones, "dropoff")
    return (
        trips.join(pickup_zones, F.col("PULocationID") == F.col("pickup_location_id"), "left")
        .join(dropoff_zones, F.col("DOLocationID") == F.col("dropoff_location_id"), "left")
        .drop("pickup_location_id", "dropoff_location_id")
    )


def revenue_by_zone_month(trips: DataFrame) -> DataFrame:
    """Output 1: revenue and trip count per pickup borough/zone, per month."""
    return (
        trips.groupBy(
            F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month"),
            "pickup_borough",
            "pickup_zone",
        )
        .agg(
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.count(F.lit(1)).alias("trip_count"),
        )
        .orderBy("pickup_month", F.desc("revenue"))
    )


def trips_by_hour_and_weekday(trips: DataFrame) -> DataFrame:
    """Output 2: trip count and average duration for each hour of each day of the week."""
    return (
        trips.groupBy(
            "pickup_day_of_week",
            # Grouping by the name too is free (it's determined by the day number)
            # and makes the output readable without a lookup.
            F.date_format("tpep_pickup_datetime", "EEE").alias("pickup_day_name"),
            "pickup_hour",
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("pickup_day_of_week", "pickup_hour")
    )


def tip_by_payment_type(trips: DataFrame) -> DataFrame:
    """Output 3: average tip percentage by payment type.

    The median is included alongside the mean because a few huge tips can pull an
    average up a lot. Cash tips aren't recorded in this dataset, so cash shows ~0%.
    """
    names = F.create_map(*[F.lit(x) for x in chain(*PAYMENT_TYPE_NAMES.items())])
    return (
        trips.groupBy("payment_type")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_percentage"),
            # percentile_approx: an exact median would need a full sort of 40M values.
            F.round(F.percentile_approx("tip_percentage", 0.5), 2).alias("median_tip_percentage"),
        )
        .withColumn("payment_type_name", names[F.col("payment_type")])
        .select(
            "payment_type",
            "payment_type_name",
            "trip_count",
            "avg_tip_percentage",
            "median_tip_percentage",
        )
        .orderBy("payment_type")
    )


def top_zone_pairs(trips: DataFrame, n: int = 20) -> DataFrame:
    """Output 4: the n busiest pickup -> dropoff zone pairs.

    Grouped by location ID, not just name: a few zones share a name (e.g. IDs 103-105 are
    all "Governor's Island/Ellis Island/Liberty Island").
    """
    return (
        trips.groupBy(
            "PULocationID",
            "pickup_borough",
            "pickup_zone",
            "DOLocationID",
            "dropoff_borough",
            "dropoff_zone",
        )
        .agg(F.count(F.lit(1)).alias("trip_count"))
        # Tie-break on IDs so the top n is the same on every run.
        .orderBy(F.desc("trip_count"), "PULocationID", "DOLocationID")
        .limit(n)
    )


def build_output_tables(enriched_trips: DataFrame) -> dict[str, DataFrame]:
    """All output tables, keyed by the folder name they're written to. Lazy: nothing runs."""
    return {
        "revenue_by_zone_month": revenue_by_zone_month(enriched_trips),
        "trips_by_hour_and_weekday": trips_by_hour_and_weekday(enriched_trips),
        "tip_by_payment_type": tip_by_payment_type(enriched_trips),
        "top_zone_pairs": top_zone_pairs(enriched_trips),
    }
