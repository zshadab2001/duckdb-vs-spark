"""Timing harness.

Every case runs the same work twice: once in a single process, once through a
distributed engine running locally. The ratio is what distributing that job costs.


An untimed warm-up runs first so a cold JVM is never measured. Both engines must
materialise a real result, and both results are compared. A failure on one engine
is recorded rather than aborting the run.
"""
import json, os, time, statistics, traceback
from dataclasses import dataclass, field
from typing import Optional, Callable, List
import pandas as pd
from . import config as C


@dataclass
class Case:
    id: str
    name: str
    category: str
    sql: Optional[str] = None              # identical SQL for both engines
    duck_sql: Optional[str] = None         # engine-specific SQL
    spark_sql: Optional[str] = None
    duck_fn: Optional[Callable] = None     # arbitrary python (writes, etc.)
    spark_fn: Optional[Callable] = None
    approximate: bool = False              # sketching algorithms, answers will not match
    metadata_only: bool = False            # answerable from the Parquet footer, no data read
    note: str = ""

    @property
    def identical_sql(self) -> bool:
        return self.sql is not None


class Bench:
    def __init__(self, duck, spark, notebook: str, size: int = None):
        self.duck = duck
        self.spark = spark
        self.notebook = notebook
        self.size = size or C.MAIN_SIZE
        self.rows: List[dict] = []

    def _duck_callable(self, case):
        if case.duck_fn:
            return case.duck_fn
        sql = case.sql or case.duck_sql
        if sql is None:
            return None
        return lambda: self.duck.execute(sql).df()

    def _spark_callable(self, case):
        if self.spark is None:
            return None
        if case.spark_fn:
            return case.spark_fn
        sql = case.sql or case.spark_sql
        if sql is None:
            return None
        return lambda: self.spark.sql(sql).toPandas()

    @staticmethod
    def _time(fn):
        # The warm-up also pulls the file into the OS page cache, so every timing
        # below is a warm-cache number for both engines. Cold-start reads from disk
        # would be slower for both and are not what this measures.
        for _ in range(C.WARMUP_RUNS):
            fn()
        times, out = [], None
        for _ in range(C.TIMED_RUNS):
            t0 = time.perf_counter()
            out = fn()
            times.append(time.perf_counter() - t0)
        return statistics.median(times), out

    # Relative tolerance for float comparison. The two engines sum in different
    # orders (different parallel partitioning), so identical logic still produces
    # slightly different last bits. Over tens of millions of rows that drift is
    # real. 1e-7 is loose enough to absorb it and still many orders of magnitude
    # tighter than any genuine logic error would be.
    FLOAT_RTOL = 1e-7
    FLOAT_ATOL = 1e-6

    @classmethod
    def _same_answer(cls, d, s):
        """Checksum, not proof.

        Compares row count and the column sums of every numeric column. Sums are
        order independent, so a different row order still passes, which is what we
        want. Two caveats worth knowing before quoting this:
          - a result with no numeric columns is only checked on row count
          - two genuinely different results could in principle share a checksum
        """
        if not isinstance(d, pd.DataFrame) or not isinstance(s, pd.DataFrame):
            return None
        if len(d) != len(s):
            return False
        dn = d.select_dtypes("number").sum().sort_index()
        sn = s.select_dtypes("number").sum().sort_index()
        if list(dn.index) != list(sn.index):
            return False
        for k in dn.index:
            a, b = float(dn[k]), float(sn[k])
            if abs(a - b) > max(cls.FLOAT_ATOL, abs(a) * cls.FLOAT_RTOL):
                return False
        return True

    def run(self, case: Case, quiet=False):
        d_fn, s_fn = self._duck_callable(case), self._spark_callable(case)
        row = {
            "notebook": self.notebook, "id": case.id, "operation": case.name,
            "category": case.category, "rows": self.size,
            "identical_sql": case.identical_sql, "note": case.note,
            "metadata_only": case.metadata_only,
            "duckdb_s": None, "spark_s": None, "ratio": None,
            "same_answer": None, "duckdb_error": None, "spark_error": None,
        }
        d_out = s_out = None

        try:
            row["duckdb_s"], d_out = self._time(d_fn)
        except Exception as e:
            row["duckdb_error"] = str(e)[:400]

        if s_fn is not None:
            try:
                row["spark_s"], s_out = self._time(s_fn)
            except Exception as e:
                row["spark_error"] = str(e)[:400]

        if row["duckdb_s"] and row["spark_s"]:
            row["ratio"] = round(row["spark_s"] / row["duckdb_s"], 2)
        row["same_answer"] = None if case.approximate else self._same_answer(d_out, s_out)
        if case.approximate:
            row["note"] = (row["note"] + " | approximate algorithm: answers are not expected to match exactly").strip(" |")
        self.rows.append(row)

        if not quiet:
            self._print(row)
        return row, d_out, s_out

    @staticmethod
    def _print(r):
        flag = "" if r["identical_sql"] else "   [engine-specific SQL]"
        print(f"{r['id']:>4}  {r['operation']}{flag}")
        if r["duckdb_error"]:
            print(f"        DuckDB : FAILED - {r['duckdb_error'][:120]}")
        else:
            print(f"        DuckDB : {r['duckdb_s']:9.3f} s")
        if r["spark_error"]:
            print(f"        Spark  : FAILED - {r['spark_error'][:120]}")
        elif r["spark_s"]:
            same = {True: "same answer", False: "ANSWERS DIFFER", None: "approximate - not compared"}[r["same_answer"]]
            print(f"        Spark  : {r['spark_s']:9.3f} s     -> DuckDB {r['ratio']}x faster   ({same})")
        print()

    def table(self):
        return pd.DataFrame(self.rows)

    def save(self):
        path = os.path.join(C.RESULTS_DIR, f"{self.notebook}.json")
        with open(path, "w") as f:
            json.dump(self.rows, f, indent=1)
        print(f"Saved {len(self.rows)} results -> results/{self.notebook}.json")
        return path


def load_all():
    """Combine every notebook's saved results."""
    frames = []
    for name in sorted(os.listdir(C.RESULTS_DIR)):
        if name.endswith(".json"):
            with open(os.path.join(C.RESULTS_DIR, name)) as f:
                data = json.load(f)
            if data:
                frames.append(pd.DataFrame(data))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
