"""Charts and tables."""
import matplotlib.pyplot as plt
import pandas as pd
from . import config as C

DUCK_C = "#3A7CA5"
SPARK_C = "#E8833A"
GREY = "#C9CBD1"


def _clean(df):
    return df[df["duckdb_s"].notna() & df["spark_s"].notna()].copy()


def times_chart(df, title=None, height_per_row=0.55):
    """Side-by-side seconds per operation."""
    d = _clean(df)
    if d.empty:
        print("Nothing to chart (Spark did not produce results).")
        return
    d = d.sort_values("ratio", ascending=False)
    n = len(d)
    fig, ax = plt.subplots(figsize=(11, max(3, height_per_row * n + 1.8)))
    y, h = range(n), 0.38
    ax.barh([i + h / 2 for i in y], d["spark_s"], height=h, color=SPARK_C, label="Distributed (Spark, local mode)")
    ax.barh([i - h / 2 for i in y], d["duckdb_s"], height=h, color=DUCK_C, label="Single process (DuckDB)")
    top = float(d["spark_s"].max())
    for i in range(n):
        sv, dv, r = float(d["spark_s"].iloc[i]), float(d["duckdb_s"].iloc[i]), d["ratio"].iloc[i]
        ax.text(sv + top * 0.012, i + h / 2, f"{sv:.1f}s", va="center", fontsize=9)
        ax.text(dv + top * 0.012, i - h / 2, f"{dv:.2f}s  ({r:g}x)", va="center",
                fontsize=9, weight="bold", color=DUCK_C)
    ax.set_xlim(0, top * 1.35)
    ax.set_yticks(list(y))
    ax.set_yticklabels(d["operation"], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Seconds (lower is better)")
    ax.set_title(title or f"{C.human(int(d['rows'].iloc[0]))} sales rows, "
                          f"both engines, {C.ENGINE_MEMORY_MB} MB and {C.plural(C.ENGINE_THREADS, 'thread')} each",
                 fontsize=12, weight="bold", pad=12)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.10 - 0.4 / max(n, 1)),
              ncol=2, frameon=False)
    ax.grid(axis="x", alpha=.25)
    for sp in ("top", "right", "left"):
        ax.spines[sp].set_visible(False)
    plt.tight_layout()
    plt.show()


def results_table(df):
    d = df.copy()
    if "metadata_only" in d:
        d["operation"] = [f"{op} (file stats only)" if m else op
                          for op, m in zip(d["operation"], d["metadata_only"].fillna(False))]
    d["Single process (s)"] = d["duckdb_s"].round(3)
    d["Distributed (s)"] = d["spark_s"].round(2)
    # Deliberately called "Ratio" and not "cost of distributing". The gap includes
    # implementation and runtime differences between the engines, not only the
    # coordination overhead, so naming it as a cause would overstate what was measured.
    d["Ratio"] = d["ratio"].map(lambda r: f"{r:g}x" if pd.notna(r) else "-")
    d["Same SQL?"] = d["identical_sql"].map({True: "yes", False: "no"})
    d["Same answer?"] = d["same_answer"].map({True: "yes", False: "NO", None: "approx"})
    cols = ["id", "operation", "category", "Single process (s)", "Distributed (s)",
            "Ratio", "Same SQL?", "Same answer?"]
    return d[cols].rename(columns={"id": "#", "operation": "Operation", "category": "Category"})


def headline(df):
    d = _clean(df)
    if d.empty:
        print("No comparable results.")
        return
    faster_distributed = d[d["ratio"] < 1]
    print("   Single process : DuckDB, in this notebook's process")
    print("   Distributed    : Spark, local mode on this same machine")
    print()
    print(f"   Operations run both ways : {len(d)}")
    print(f"   Identical SQL both ways  : {int(df['identical_sql'].sum())} of {len(df)}")
    print(f"   Slower when distributed  : {d['ratio'].min():g}x to {d['ratio'].max():g}x")
    print(f"   Faster when distributed  : {len(faster_distributed)} "
          f"({', '.join(faster_distributed['operation'].head(4)) if len(faster_distributed) else 'none at this size'})")
    bad = df[df["same_answer"] == False]
    if len(bad):
        print(f"   WARNING - answers differ on: {', '.join(bad['operation'])}")
