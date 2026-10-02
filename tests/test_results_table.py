from scripts.results_table import markdown_table


def row(experiment, variant, seconds, run_date="2026-10-02T10:00:00"):
    return {
        "experiment": experiment,
        "variant": variant,
        "median_seconds": seconds,
        "run_date": run_date,
    }


def test_table_uses_latest_run_and_computes_speedup():
    rows = [
        row("caching", "baseline", "99.00", run_date="2026-10-01T09:00:00"),  # older, ignored
        row("caching", "baseline", "20.00"),
        row("caching", "optimized", "10.00"),
        row("shuffle", "baseline", "9.00"),
        row("shuffle", "optimized", "3.00"),
        row("shuffle", "aqe_on", "4.50"),
    ]

    lines = markdown_table(rows).splitlines()

    assert lines[2:] == [
        "| caching | optimized | 20.00 | 10.00 | 2.00x |",
        "| shuffle | optimized | 9.00 | 3.00 | 3.00x |",
        "| shuffle | aqe_on | 9.00 | 4.50 | 2.00x |",
    ]
