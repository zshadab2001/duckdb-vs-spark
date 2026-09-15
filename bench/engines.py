"""Starts both engines with identical memory and thread budgets, and registers the
same table names in each so most cases can send one SQL string to both.

Spark runs in local mode, so it does all the extra work of splitting a job up
with no extra machines to gain from."""
import os, time, warnings, duckdb
from . import config as C

warnings.filterwarnings("ignore")

_spark = None
SPARK_START_SECONDS = None
DUCK_START_SECONDS = None


def get_duckdb(memory_mb=None, threads=None):
    global DUCK_START_SECONDS
    t0 = time.perf_counter()
    con = duckdb.connect(config={
        "memory_limit": f"{memory_mb or C.ENGINE_MEMORY_MB}MB",
        "threads": str(threads or C.ENGINE_THREADS),
    })
    DUCK_START_SECONDS = time.perf_counter() - t0
    return con


def get_spark(quiet=True):
    """Returns a SparkSession, or None if Spark can't run here."""
    global _spark, SPARK_START_SECONDS
    if _spark is not None:
        return _spark
    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        print("PySpark not available:", e)
        return None
    try:
        t0 = time.perf_counter()
        b = SparkSession.builder.appName("duckdb_vs_spark").master(f"local[{C.ENGINE_THREADS}]")
        for k, v in C.SPARK_CONF.items():
            b = b.config(k, v)
        _spark = b.getOrCreate()
        if quiet:
            _spark.sparkContext.setLogLevel("FATAL")
        _spark.sql("SELECT 1").collect()          # force it to be genuinely ready
        SPARK_START_SECONDS = time.perf_counter() - t0
        return _spark
    except Exception as e:
        print("Spark failed to start:", str(e)[:300])
        return None


def stop_spark():
    global _spark
    if _spark is not None:
        try:
            _spark.stop()
        except Exception:
            pass
        _spark = None


VIEW_KEYS = ("sales", "customers", "products", "returns", "sales_v2")


def attach(duck, spark, paths, extras=True):
    """Point both engines at the same files, under the same names."""
    registered = []
    for name in VIEW_KEYS:
        path = paths.get(name)
        if not path or not os.path.exists(path):
            continue
        if not extras and name in ("returns", "sales_v2"):
            continue
        duck.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
        if spark is not None:
            spark.read.parquet(path).createOrReplaceTempView(name)
        registered.append(name)

    # Needs hive-style partition discovery in both engines.
    part = paths.get("sales_part")
    if part and os.path.isdir(part):
        duck.execute(
            f"CREATE OR REPLACE VIEW sales_part AS "
            f"SELECT * FROM read_parquet('{part}/**/*.parquet', hive_partitioning=true)")
        if spark is not None:
            spark.read.parquet(part).createOrReplaceTempView("sales_part")
        registered.append("sales_part")
    return registered


def describe():
    lines = ["ENGINE STARTUP", ""]
    if DUCK_START_SECONDS is not None:
        lines.append(f"   DuckDB ready in : {DUCK_START_SECONDS:.3f} s")
    if SPARK_START_SECONDS is not None:
        lines.append(f"   Spark  ready in : {SPARK_START_SECONDS:.1f} s   (one machine, no cluster)")
        lines.append("")
        lines.append("   Startup is not included in any timing below.")
    return "\n".join(lines)
