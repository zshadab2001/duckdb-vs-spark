"""
Every setting that could affect a number lives here.

Nothing is hidden anywhere else. If you think a setting is unfair to either
engine, change it here and re-run — that is the point of this file.
"""
import os, shutil

# ---------------------------------------------------------------------------
# 1. HOW MUCH DATA
# ---------------------------------------------------------------------------
# QUICK=1 is a small fast run to check everything works (a few minutes).
# QUICK=0 is the real run.
def _env(name, default=""):
    """Read an environment variable.

    Note the `.strip()` and the emptiness check. CI systems often set a variable
    to an EMPTY STRING rather than leaving it unset, and plain os.environ.get()
    returns that empty string instead of the default. That difference will
    happily crash int() on the first import.
    """
    v = os.environ.get(name)
    v = v.strip() if v is not None else ""
    return v if v else default


def _env_int(name, default):
    v = _env(name)
    try:
        return int(v) if v else default
    except ValueError:
        print(f"[config] {name}={v!r} is not a number; using {default}")
        return default


QUICK = _env("BENCH_QUICK", "1") != "0"

if QUICK:
    SIZES = [200_000, 1_000_000]
else:
    SIZES = [1_000_000, 5_000_000, 20_000_000, 50_000_000]

# The size used for the operation matrix. Every operation runs at this size.
MAIN_SIZE = SIZES[-1]


def _total_ram_gb():
    try:
        import psutil
        return psutil.virtual_memory().total / 1024**3
    except Exception:
        try:
            return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024**3
        except Exception:
            return 8.0


RAM_GB = _total_ram_gb()
CORES = os.cpu_count() or 4
try:
    FREE_DISK_GB = shutil.disk_usage(".").free / 1024**3
except Exception:
    FREE_DISK_GB = None

# ---------------------------------------------------------------------------
# 2. HOW MUCH MACHINE EACH ENGINE GETS  --  THEY GET THE SAME
# ---------------------------------------------------------------------------
# This is the most important fairness rule in the project. Neither engine is
# given a bigger desk than the other.
ENGINE_MEMORY_MB = _env_int("BENCH_MEM_MB", max(1024, int(RAM_GB * 1024 * 0.35)))
ENGINE_THREADS = _env_int("BENCH_THREADS", CORES)

# ---------------------------------------------------------------------------
# 3. FAIRNESS RULES
# ---------------------------------------------------------------------------
# One untimed warm-up run before timing, so we never time a cold JVM.
WARMUP_RUNS = 1
# No repeats: the gaps we are measuring are far larger than cloud-runner noise.
# For variance, re-run the whole workflow on a different day instead.
TIMED_RUNS = _env_int("BENCH_RUNS", 1)

# ---------------------------------------------------------------------------
# 4. SPARK TUNING -- every one of these makes Spark FASTER than its defaults
# ---------------------------------------------------------------------------
SPARK_CONF = {
    "spark.driver.memory": f"{ENGINE_MEMORY_MB}m",
    # THE BIG ONE. Spark's default is 200. On a single machine that means 200
    # lots of paperwork for one desk. Matching it to cores is what makes this
    # comparison fair rather than a hatchet job.
    "spark.sql.shuffle.partitions": str(ENGINE_THREADS),
    # Fast handover of results to Python. Off by default.
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    # Copy small lookup tables to workers instead of shuffling them.
    # DuckDB does this automatically, so leaving it off would handicap Spark.
    "spark.sql.autoBroadcastJoinThreshold": str(128 * 1024 * 1024),
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    # Spark's own best feature: re-planning while running.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.sql.parquet.enableVectorizedReader": "true",
    "spark.ui.enabled": "false",
    "spark.driver.maxResultSize": "1g",
    "spark.log.level": "ERROR",
}

# ---------------------------------------------------------------------------
# 5. WHERE THINGS LIVE
# ---------------------------------------------------------------------------
ROOT = _env("BENCH_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
DATA_DIR = os.path.join(ROOT, "bench_data")
OUT_DIR = os.path.join(ROOT, "bench_out")
RESULTS_DIR = os.path.join(ROOT, "results")

for _d in (DATA_DIR, OUT_DIR, RESULTS_DIR):
    os.makedirs(_d, exist_ok=True)


def human(n):
    return f"{n:,}"


def plural(n, word):
    """So the report never says '1 threads'."""
    return f"{n} {word}" + ("" if n == 1 else "s")


def summary():
    lines = [
        "SETTINGS IN FORCE",
        "",
        f"   Mode                 : {'QUICK (small, for checking it works)' if QUICK else 'FULL RUN'}",
        f"   Data sizes           : {[human(s) for s in SIZES]}",
        f"   Operations run at    : {human(MAIN_SIZE)} sales rows",
        f"   Memory PER ENGINE    : {ENGINE_MEMORY_MB} MB   <- identical for both",
        f"   Threads PER ENGINE   : {plural(ENGINE_THREADS, 'thread')}   <- identical for both",
        f"   Warm-up / timed runs : {WARMUP_RUNS} / {TIMED_RUNS}",
        f"   Machine              : {plural(CORES, 'thread')}, {RAM_GB:.1f} GB RAM"
        + (f", {FREE_DISK_GB:.1f} GB free disk" if FREE_DISK_GB else ""),
    ]
    return "\n".join(lines)
