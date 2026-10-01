"""Download NYC TLC yellow taxi trip data (Parquet) and the taxi zone lookup CSV.

Source page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Files are served from the TLC's CloudFront bucket (URLs verified 2026-10-01).

Usage (from the project root):
    python -m scripts.download_data                  # all 12 months of 2024 + zone lookup
    python -m scripts.download_data --months 1       # just January (quick start)
    python -m scripts.download_data --year 2023 --months 1 2 3
"""

import argparse
import shutil
import sys
import urllib.request
from pathlib import Path

from src import config

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
CHUNK_SIZE = 1024 * 1024  # 1 MB


def trip_url(year: int, month: int) -> str:
    return f"{BASE_URL}/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


def zone_lookup_url() -> str:
    return f"{BASE_URL}/misc/taxi_zone_lookup.csv"


def download(url: str, dest: Path) -> bool:
    """Download url to dest. Returns True if downloaded, False if skipped (already present)."""
    if dest.exists():
        print(f"  skip  {dest.name} (already downloaded)")
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temporary .part file and rename only when complete, so an interrupted
    # download is never mistaken for a finished one on the next run.
    tmp = dest.with_name(dest.name + ".part")

    request = urllib.request.Request(url, headers={"User-Agent": "nyc-taxi-spark-performance"})
    with urllib.request.urlopen(request, timeout=60) as response, open(tmp, "wb") as out:
        size_mb = int(response.headers.get("Content-Length", 0)) / 1e6
        print(f"  fetch {dest.name} ({size_mb:.1f} MB) ...", flush=True)
        shutil.copyfileobj(response, out, CHUNK_SIZE)

    tmp.replace(dest)
    return True


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--year", type=int, default=config.YEAR)
    parser.add_argument(
        "--months",
        type=int,
        nargs="+",
        default=list(range(1, 13)),
        choices=range(1, 13),
        metavar="MONTH",
        help="Month numbers to download (default: all 12)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    print(f"Downloading into {config.RAW_DIR}")

    jobs = [(zone_lookup_url(), config.ZONE_LOOKUP_FILE)]
    jobs += [(trip_url(args.year, m), config.trip_file(args.year, m)) for m in args.months]

    downloaded, failed = 0, 0
    for url, dest in jobs:
        try:
            downloaded += download(url, dest)
        except Exception as exc:  # keep going so one bad month doesn't block the rest
            print(f"  FAIL  {dest.name}: {exc}", file=sys.stderr)
            failed += 1

    skipped = len(jobs) - downloaded - failed
    print(f"Done: {downloaded} downloaded, {skipped} already present, {failed} failed.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
