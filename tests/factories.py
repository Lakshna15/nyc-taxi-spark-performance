"""Builders for tiny hand-made DataFrames shared by the tests."""

from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

# Only the columns the pipeline uses, with the same types as the real TLC files
# (the timestamps in the Parquet files are read as timestamp_ntz).
TRIP_SCHEMA = StructType(
    [
        StructField("trip_id", StringType()),  # test-only label so assertions can name rows
        StructField("tpep_pickup_datetime", TimestampNTZType()),
        StructField("tpep_dropoff_datetime", TimestampNTZType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
    ]
)

ZONE_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType()),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),
        StructField("service_zone", StringType()),
    ]
)


def trip(trip_id: str, **overrides) -> dict:
    """A valid trip (Friday 2024-03-15, 08:00-08:30). Override fields to change it."""
    row = {
        "trip_id": trip_id,
        "tpep_pickup_datetime": datetime(2024, 3, 15, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 3, 15, 8, 30),
        "PULocationID": 161,
        "DOLocationID": 236,
        "trip_distance": 3.2,
        "payment_type": 1,
        "fare_amount": 20.0,
        "tip_amount": 4.0,
        "total_amount": 28.5,
    }
    row.update(overrides)
    return row


def make_trips(spark, rows: list[dict]):
    return spark.createDataFrame(
        [tuple(r[field.name] for field in TRIP_SCHEMA.fields) for r in rows], TRIP_SCHEMA
    )


def make_zones(spark):
    """A handful of real rows from taxi_zone_lookup.csv."""
    return spark.createDataFrame(
        [
            (132, "Queens", "JFK Airport", "Airports"),
            (161, "Manhattan", "Midtown Center", "Yellow Zone"),
            (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
            (237, "Manhattan", "Upper East Side South", "Yellow Zone"),
        ],
        ZONE_SCHEMA,
    )
