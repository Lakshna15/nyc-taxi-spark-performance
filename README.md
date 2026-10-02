# nyc-taxi-spark-performance

Processes a year of NYC TLC Yellow Taxi trip data with PySpark and measures how common
Spark optimizations affect performance.

> Work in progress. The full write-up, architecture diagram, and benchmark results come
> in a later phase. All numbers will come from real runs.

## Requirements

- Python 3.10–3.12 (PySpark 3.5 does not support newer versions)
- Java 17
- GNU make

### Extra step on Windows

Spark uses Hadoop's file system layer, which needs two native helpers (`winutils.exe` and
`hadoop.dll`) to **write** files on Windows. Reading works without them. Apache doesn't ship
Windows builds, so most people use the community builds from
[cdarlint/winutils](https://github.com/cdarlint/winutils):

1. Download `winutils.exe` and `hadoop.dll` from the `hadoop-3.3.6/bin` folder into `C:\hadoop\bin`.
2. Set `HADOOP_HOME=C:\hadoop` and add `C:\hadoop\bin` to your `PATH`.
3. Open a new terminal.

macOS and Linux need none of this.

## Quick start

```bash
make setup      # create .venv and install dependencies
make download   # ~650 MB of Parquet into data/raw/ (already-downloaded files are skipped)
make run        # clean + join + aggregate, writes Parquet tables to data/output/
make test
```

## Author & Contributors

- **Lakshna** ([@Lakshna15](https://github.com/Lakshna15))

