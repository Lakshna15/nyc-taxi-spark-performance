def test_spark_runs_a_tiny_job(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "letter"])
    assert df.filter(df.id > 1).count() == 2


def test_session_time_zone_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_extra_conf_is_applied(spark):
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
