# Compare DuckDB and Spark

A like-for-like timing comparison of **46 data operations** run on one ordinary machine, using
two engines: **DuckDB**, which runs inside a single process, and **Apache Spark**, which is
built to spread work across many machines.

## The question

The interesting question is not which engine is better. It is which jobs sit on which side of
the line.

## The finding

The gap varies enormously depending on what the job does. A row count and a wide shuffle join
are not the same kind of work, and treating them as though they need the same tool is where the
cost hides.

## What is measured

| Notebook | Covers |
|---|---|
| `00_setup_and_data` | Settings, rules, and building the test data |
| `01_scan_and_filter` | Counting, filtering, column pruning, partition pruning |
| `02_aggregation` | Group-bys, rollups, distinct counts, percentiles, top-N |
| `03_joins` | Star-schema, big-to-big, anti-join, self-join, semi-join |
| `04_windows` | Ranking, running totals, rolling averages, lag/lead |
| `05_etl_transform` | Dedup, snapshot diff, pivot, regex, JSON, business rules |
| `06_data_quality` | Null profiles, duplicates, range checks, referential integrity |
| `07_features` | Feature tables, time windows, group statistics, RFM |
| `08_write_output` | Writing files, partitioned writes, compaction, CSV to Parquet |
| `09_duckdb_scale` | DuckDB only: how far one machine goes as the table grows |
| `10_summary` | Everything combined |

## The rules

1. Both engines read the same Parquet files from the same folder.
2. Tables are registered under identical names in both, so most cases send one identical SQL
   string to each. Where that is impossible the case is marked and both versions are shown.
3. Both engines get the same memory and thread count.
4. One untimed warm-up runs first, so a cold JVM is never measured.
5. Spark's startup time is reported but never added to a timing.
6. Both results are compared. A fast wrong answer is worthless.
7. Every Spark setting changed from its default makes Spark faster, notably
   `shuffle.partitions` (the default of 200 schedules 200 tasks across a handful of cores) and
   Arrow for result handover. All of them are in [`bench/config.py`](bench/config.py).
