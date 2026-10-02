import csv

from src import benchmark, config
from src.experiments import Experiment, Variant, explain_string, spark_conf
from src.transform import join_zones
from tests.factories import make_trips, make_zones, trip

SHUFFLE_KEY = "spark.sql.shuffle.partitions"


def test_spark_conf_applies_then_restores(spark):
    before = spark.conf.get(SHUFFLE_KEY)

    with spark_conf(spark, {SHUFFLE_KEY: "7"}):
        assert spark.conf.get(SHUFFLE_KEY) == "7"

    assert spark.conf.get(SHUFFLE_KEY) == before


def test_spark_conf_unsets_keys_that_were_not_set(spark):
    key = "spark.sql.autoBroadcastJoinThreshold"
    spark.conf.unset(key)
    default = spark.conf.get(key)

    with spark_conf(spark, {key: "-1"}):
        assert spark.conf.get(key) == "-1"

    assert spark.conf.get(key) == default


def test_time_experiment_runs_each_variant_n_times_with_its_conf(spark):
    seen = []

    def record(s):
        seen.append(s.conf.get(SHUFFLE_KEY))

    experiment = Experiment(
        name="demo",
        description="demo",
        variants=[
            Variant("baseline", record, {SHUFFLE_KEY: "5"}),
            Variant("optimized", record, {SHUFFLE_KEY: "9"}),
        ],
    )
    times = benchmark.time_experiment(spark, experiment, runs=3)

    assert {name: len(t) for name, t in times.items()} == {"baseline": 3, "optimized": 3}
    assert seen == ["5", "9"] * 3  # interleaved, each with its own setting


def test_append_results_writes_header_once(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RESULTS_FILE", tmp_path / "results.csv")
    experiment = Experiment("demo", "demo", variants=[])
    specs = {f: "x" for f in benchmark.RESULT_FIELDS[5:]}

    benchmark.append_results(experiment, {"baseline": [3.0, 1.0, 2.0]}, specs, "2026-01-01")
    benchmark.append_results(experiment, {"optimized": [1.0, 1.0, 1.0]}, specs, "2026-01-01")

    with open(tmp_path / "results.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["variant"] for r in rows] == ["baseline", "optimized"]
    assert rows[0]["median_seconds"] == "2.00"
    assert rows[0]["runs_seconds"] == "3.00;1.00;2.00"


def test_broadcast_hint_beats_disabled_auto_broadcast(spark):
    """The broadcast-join experiment relies on this: the hint wins over threshold = -1."""
    trips, zones = make_trips(spark, [trip("t")]), make_zones(spark)

    with spark_conf(spark, {"spark.sql.autoBroadcastJoinThreshold": "-1"}):
        plain = explain_string(join_zones(trips, zones), mode="simple")
        hinted = explain_string(join_zones(trips, zones, broadcast_zones=True), mode="simple")

    assert "SortMergeJoin" in plain and "BroadcastHashJoin" not in plain
    assert "BroadcastHashJoin" in hinted and "SortMergeJoin" not in hinted
