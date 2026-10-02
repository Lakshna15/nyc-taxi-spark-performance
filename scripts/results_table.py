"""Print the benchmark results as a Markdown table, ready to paste into the README.

Uses the most recent run of each experiment/variant in results/benchmark_results.csv, so
the README always shows real measured numbers rather than hand-typed ones.

Usage: python -m scripts.results_table
"""

import csv
from pathlib import Path

from src import config


def latest_results(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    """Keep the newest row per (experiment, variant). ISO dates sort correctly as strings."""
    latest: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = (row["experiment"], row["variant"])
        if key not in latest or row["run_date"] > latest[key]["run_date"]:
            latest[key] = row
    return latest


def markdown_table(rows: list[dict[str, str]]) -> str:
    latest = latest_results(rows)
    lines = [
        "| Experiment | Variant | Baseline (s) | This variant (s) | Speedup |",
        "|---|---|---:|---:|---:|",
    ]
    experiments = list(dict.fromkeys(experiment for experiment, _ in latest))  # keep CSV order
    for experiment in experiments:
        baseline = float(latest[(experiment, "baseline")]["median_seconds"])
        for (exp, variant), row in latest.items():
            if exp != experiment or variant == "baseline":
                continue
            seconds = float(row["median_seconds"])
            lines.append(
                f"| {experiment} | {variant} | {baseline:.2f} | {seconds:.2f} "
                f"| {baseline / seconds:.2f}x |"
            )
    return "\n".join(lines)


def main(path: Path = config.RESULTS_FILE) -> None:
    with open(path, newline="") as f:
        print(markdown_table(list(csv.DictReader(f))))


if __name__ == "__main__":
    main()
