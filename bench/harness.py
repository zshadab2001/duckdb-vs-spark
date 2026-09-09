"""
The stopwatch, and the rules it follows.

  * one untimed WARM-UP run first, so we never time a cold JVM
  * both engines must hand back a REAL answer, not a promise to do the work later
  * both answers are kept and compared, because a fast wrong answer is worthless
  * if one engine fails a case, we record the failure and carry on

Most cases send the SAME SQL STRING to both engines. Where that is impossible
(JSON handling, percentiles, writing files) the case is marked, and the count of
identical-SQL cases is reported at the end. That count is itself a finding.
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
    materialize: str = "df"                # "df" = collect result, "count" = count rows
    approximate: bool = False              # answers legitimately differ (approx algorithms)
    note: str = ""
    why: str = ""                          # what this case is actually testing

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

    # -- internals ---------------------------------------------------------
    def _duck_callable(self, case):
        if case.duck_fn:
            return case.duck_fn
        sql = case.sql or case.duck_sql
        if sql is None:
            return None
        if case.materialize == "count":
            return lambda: self.duck.execute(
                f"SELECT count(*) AS n FROM ({sql}) _t").df()
        return lambda: self.duck.execute(sql).df()

    def _spark_callable(self, case):
        if self.spark is None:
            return None
        if case.spark_fn:
            return case.spark_fn
        sql = case.sql or case.spark_sql
        if sql is None:
            return None
        if case.materialize == "count":
            return lambda: pd.DataFrame({"n": [self.spark.sql(sql).count()]})
        return lambda: self.spark.sql(sql).toPandas()

    @staticmethod
    def _time(fn):
        for _ in range(C.WARMUP_RUNS):
            fn()
        times, out = [], None
        for _ in range(C.TIMED_RUNS):
            t0 = time.perf_counter()
            out = fn()
            times.append(time.perf_counter() - t0)
        return statistics.median(times), out

    @staticmethod
    def _same_answer(d, s):
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
            if abs(a - b) > max(1e-6, abs(a) * 1e-9):
                return False
        return True

    # -- public ------------------------------------------------------------
    def run(self, case: Case, quiet=False):
        d_fn, s_fn = self._duck_callable(case), self._spark_callable(case)
        row = {
            "notebook": self.notebook, "id": case.id, "operation": case.name,
            "category": case.category, "rows": self.size,
            "identical_sql": case.identical_sql, "why": case.why, "note": case.note,
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
    """Combine every notebook's results for the summary notebook."""
    frames = []
    for name in sorted(os.listdir(C.RESULTS_DIR)):
        if name.endswith(".json"):
            with open(os.path.join(C.RESULTS_DIR, name)) as f:
                data = json.load(f)
            if data:
                frames.append(pd.DataFrame(data))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
