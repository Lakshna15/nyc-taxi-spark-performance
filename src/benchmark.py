"""Benchmark harness: time each experiment's variants and append the results to a CSV.

Usage:
    python -m src.benchmark                          # every experiment
    python -m src.benchmark file_format caching      # just these
    python -m src.benchmark --list                   # show experiment names
    python -m src.benchmark --hold-ui caching        # keep the Spark UI up at the end

For each experiment:
  1. prepare(): untimed one-time setup, e.g. writing a CSV copy (skipped if it exists).
  2. notes(): save query plans and Spark UI metrics to docs/plans/<experiment>.txt.
     This also runs each query once, which warms up the JVM and OS file cache.
  3. Run the variants interleaved (baseline, optimized, baseline, optimized, ...) so any
     drift affects both equally, clearing Spark's cache before every run. Record the
     median of RUNS_PER_VARIANT wall-clock times.
"""

import argparse
import csv
import logging
import os
import platform
import statistics
import time
from datetime import datetime
from pathlib import Path

import psutil
from pyspark.sql import SparkSession

from src import config
from src.experiments import Experiment, build_experiments, spark_conf
from src.spark_session import build_spark

log = logging.getLogger(__name__)

RUNS_PER_VARIANT = 3

RESULT_FIELDS = [
    "experiment",
    "variant",
    "median_seconds",
    "runs_seconds",
    "run_date",
    "cpu_model",
    "logical_cores",
    "ram_gb",
    "os",
    "runtime",
    "spark_version",
    "spark_master",
    "driver_memory",
]


def _cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as cpuinfo:  # Linux (and Docker)
            for line in cpuinfo:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def machine_specs(spark: SparkSession) -> dict[str, str]:
    """Describe the machine so readers can put the timings in context."""
    return {
        "cpu_model": _cpu_model(),
        "logical_cores": str(os.cpu_count()),
        # Inside Docker this is the memory of Docker's VM, not the whole host.
        "ram_gb": f"{psutil.virtual_memory().total / 1024**3:.1f}",
        "os": platform.platform(terse=True),
        "runtime": "docker" if Path("/.dockerenv").exists() else "native",
        "spark_version": spark.version,
        "spark_master": spark.sparkContext.master,
        "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory", "1g"),
    }


def time_experiment(
    spark: SparkSession, experiment: Experiment, runs: int
) -> dict[str, list[float]]:
    """Run every variant `runs` times (interleaved) and return the wall-clock seconds."""
    times: dict[str, list[float]] = {v.name: [] for v in experiment.variants}
    for i in range(1, runs + 1):
        for variant in experiment.variants:
            # Start each run cold: drop anything an earlier run cached in Spark. (The OS
            # file cache can't be cleared from here; the notes step warms it for everyone.)
            spark.catalog.clearCache()
            label = f"{experiment.name} | {variant.name} | run {i}/{runs}"
            # Shows up as the job description in the Spark UI's Jobs and Stages pages.
            spark.sparkContext.setJobDescription(label)
            with spark_conf(spark, variant.conf):
                start = time.perf_counter()
                variant.workload(spark)
                elapsed = time.perf_counter() - start
            times[variant.name].append(elapsed)
            log.info("  %-48s %7.2fs", label, elapsed)
    spark.sparkContext.setJobDescription(None)
    return times


def append_results(
    experiment: Experiment, times: dict[str, list[float]], specs: dict[str, str], run_date: str
) -> None:
    path = config.RESULTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
        if is_new:
            writer.writeheader()
        for variant, seconds in times.items():
            writer.writerow(
                {
                    "experiment": experiment.name,
                    "variant": variant,
                    "median_seconds": f"{statistics.median(seconds):.2f}",
                    "runs_seconds": ";".join(f"{s:.2f}" for s in seconds),
                    "run_date": run_date,
                    **specs,
                }
            )


def save_notes(experiment: Experiment, text: str, specs: dict[str, str], run_date: str) -> Path:
    config.PLANS_DIR.mkdir(parents=True, exist_ok=True)
    path = config.PLANS_DIR / f"{experiment.name}.txt"
    header = (
        f"Experiment: {experiment.name}\n{experiment.description}\n"
        f"Captured {run_date} on {specs['cpu_model']}, {specs['logical_cores']} cores, "
        f"{specs['ram_gb']} GB RAM ({specs['runtime']}), Spark {specs['spark_version']}\n\n"
    )
    path.write_text(header + text + "\n", encoding="utf-8")
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark performance experiments.")
    parser.add_argument("experiments", nargs="*", help="Experiment names (default: all)")
    parser.add_argument("--list", action="store_true", help="List experiments and exit")
    parser.add_argument("--runs", type=int, default=RUNS_PER_VARIANT)
    parser.add_argument(
        "--hold-ui",
        action="store_true",
        help="Wait for Enter before exiting, so the Spark UI stays up for screenshots",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)

    spark = build_spark("benchmark")
    try:
        experiments = {e.name: e for e in build_experiments(spark)}
        if args.list:
            for e in experiments.values():
                print(f"{e.name:<20} {e.description}")
            return
        unknown = set(args.experiments) - set(experiments)
        if unknown:
            raise SystemExit(f"Unknown experiment(s): {', '.join(sorted(unknown))}")
        selected = [experiments[n] for n in args.experiments] or list(experiments.values())

        specs = machine_specs(spark)
        run_date = datetime.now().isoformat(timespec="seconds")
        log.info("Machine: %s", specs)

        for experiment in selected:
            log.info("=== %s: %s", experiment.name, experiment.description)
            if experiment.prepare:
                experiment.prepare(spark)
            if experiment.notes:
                spark.catalog.clearCache()
                path = save_notes(experiment, experiment.notes(spark), specs, run_date)
                log.info("  notes saved to %s", path.relative_to(config.PROJECT_ROOT))

            times = time_experiment(spark, experiment, args.runs)
            append_results(experiment, times, specs, run_date)

            baseline = statistics.median(times["baseline"])
            for variant, seconds in times.items():
                median = statistics.median(seconds)
                log.info(
                    "  %-10s median %7.2fs  (%.2fx vs baseline)", variant, median, baseline / median
                )

        if args.hold_ui:
            input(f"Spark UI: {spark.sparkContext.uiWebUrl} (or localhost:4040). Enter to exit.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
