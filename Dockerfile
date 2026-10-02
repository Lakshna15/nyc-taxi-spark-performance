# Linux runtime for the pipeline and benchmarks. On Windows this avoids Hadoop's native
# helpers (winutils.exe / hadoop.dll) that Spark otherwise needs to write files.
FROM python:3.11-slim-bookworm

# Spark runs on the JVM; PySpark 3.5 supports Java 17. procps provides `ps`, which Spark's
# launch scripts call.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The Makefile's docker-* targets bind-mount the project over /app so code edits don't need a
# rebuild; copying it in as well keeps the image runnable on its own.
COPY . .

# Spark UI (only up while a job is running)
EXPOSE 4040
