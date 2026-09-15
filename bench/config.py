"""Settings. Anything that affects a timing lives here."""
import os
import shutil


def _env(name, default=""):
    # CI sets unused workflow inputs to an empty string rather than leaving them
    # unset, and os.environ.get() returns that empty string instead of the default.
    v = os.environ.get(name)
    v = v.strip() if v is not None else ""
    return v if v else default


def _env_int(name, default):
    v = _env(name)
    try:
        return int(v) if v else default
    except ValueError:
        print(f"[config] {name}={v!r} is not a number, using {default}")
        return default


QUICK = _env("BENCH_QUICK", "1") != "0"

# Rows in the sales table. About 88 bytes/row on disk, and the companion
# datasets roughly double that, so 20M needs ~3.5 GB of data plus room for
# written output and Spark's shuffle spill. A GitHub standard runner has
# ~14 GB free, which is why 20M rather than something larger.
MAIN_SIZE = _env_int("BENCH_ROWS", 1_000_000 if QUICK else 20_000_000)


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

# Both engines get the same budget.
ENGINE_MEMORY_MB = _env_int("BENCH_MEM_MB", max(1024, int(RAM_GB * 1024 * 0.35)))
ENGINE_THREADS = _env_int("BENCH_THREADS", CORES)

WARMUP_RUNS = 1
TIMED_RUNS = _env_int("BENCH_RUNS", 1)

SPARK_CONF = {
    "spark.driver.memory": f"{ENGINE_MEMORY_MB}m",
    # Default is 200. On a single machine that schedules 200 tasks across a handful
    # of cores, which dominates the runtime of anything that shuffles.
    "spark.sql.shuffle.partitions": str(ENGINE_THREADS),
    # Off by default. Without it the timings include a slow row-by-row handover to Python.
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    # Raised so the small lookup tables get broadcast rather than shuffled. DuckDB
    # makes that choice automatically, so the default 10MB would understate Spark.
    "spark.sql.autoBroadcastJoinThreshold": str(128 * 1024 * 1024),
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.sql.parquet.enableVectorizedReader": "true",
    "spark.ui.enabled": "false",
    "spark.driver.maxResultSize": "1g",
    "spark.log.level": "ERROR",
}

ROOT = _env("BENCH_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
DATA_DIR = os.path.join(ROOT, "bench_data")
OUT_DIR = os.path.join(ROOT, "bench_out")
RESULTS_DIR = os.path.join(ROOT, "results")

for _d in (DATA_DIR, OUT_DIR, RESULTS_DIR):
    os.makedirs(_d, exist_ok=True)


def human(n):
    return f"{n:,}"


def plural(n, word):
    return f"{n} {word}" + ("" if n == 1 else "s")


def summary():
    lines = [
        "SETTINGS IN FORCE",
        "",
        f"   Mode                 : {'QUICK' if QUICK else 'FULL RUN'}",
        f"   Sales rows           : {human(MAIN_SIZE)}",
        f"   Memory per engine    : {ENGINE_MEMORY_MB} MB",
        f"   Threads per engine   : {plural(ENGINE_THREADS, 'thread')}",
        f"   Warm-up / timed runs : {WARMUP_RUNS} / {TIMED_RUNS}",
        f"   Machine              : {plural(CORES, 'thread')}, {RAM_GB:.1f} GB RAM"
        + (f", {FREE_DISK_GB:.1f} GB free disk" if FREE_DISK_GB else ""),
    ]
    need_gb = MAIN_SIZE * 175 * 2.1 / 1024**3
    lines += ["", f"   Rough disk needed    : {need_gb:.1f} GB"]
    if FREE_DISK_GB and need_gb > FREE_DISK_GB * 0.8:
        lines.append(f"   WARNING: only {FREE_DISK_GB:.1f} GB free. Lower BENCH_ROWS.")
    return "\n".join(lines)
