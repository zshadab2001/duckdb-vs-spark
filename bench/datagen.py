"""Builds the test datasets.

All synthetic and derived from a hash of the row number, so it is reproducible.

The sales table is deliberately wide (22 columns) and deliberately dirty. Quality
checks against clean data measure nothing, so the defects below are injected on
purpose and listed rather than left to be discovered.
"""
import os, duckdb
from . import config as C

DEFECTS = {
    "null customer_id": "~2%",
    "null amount": "~1%",
    "exact duplicate rows": "~0.5%",
    "negative amount (invalid)": "~0.5%",
    "orphan customer_id (no matching customer)": "~1%",
    "inconsistent country text ('UK' / 'uk' / ' Uk ')": "~30% of customers",
}


def paths(n_rows):
    tag = f"{n_rows // 1000}k" if n_rows < 1_000_000 else f"{n_rows // 1_000_000}m"
    d = C.DATA_DIR.replace("\\", "/")
    return {
        "tag": tag,
        "sales": f"{d}/sales_{tag}.parquet",
        "customers": f"{d}/customers_{tag}.parquet",
        "products": f"{d}/products_{tag}.parquet",
        # extras, only built at MAIN_SIZE
        "returns": f"{d}/returns_{tag}.parquet",
        "sales_v2": f"{d}/sales_v2_{tag}.parquet",
        "sales_part": f"{d}/sales_part_{tag}",
        "sales_small_files": f"{d}/sales_small_{tag}",
        "sales_csv": f"{d}/sales_{tag}.csv",
    }


def _con():
    return duckdb.connect(config={"memory_limit": f"{C.ENGINE_MEMORY_MB}MB",
                                  "threads": str(C.ENGINE_THREADS)})


_SALES_SELECT = """
SELECT
    i                                                              AS sale_id,
    -- ~2% null, ~1% orphan (id beyond the customer table)
    CASE WHEN hash(i * 101) % 50 = 0 THEN NULL
         WHEN hash(i * 103) % 100 = 0 THEN {n_cust} + (hash(i) % 1000)::INT
         ELSE (hash(i * 2654435761) % {n_cust})::INT END            AS customer_id,
    (hash(i * 40503) % {n_prod})::INT                               AS product_id,
    (hash(i * 7919) % 400)::INT                                     AS store_id,
    TIMESTAMP '2024-01-01' + to_seconds(((hash(i * 31) % 47520000))::BIGINT) AS sale_ts,
    DATE '2024-01-01' + ((hash(i * 31) % 550)::INT)                 AS sale_date,
    1 + (hash(i * 37) % 5)::INT                                     AS quantity,
    round(1 + (hash(i * 41) % 30000) / 100.0, 2)                    AS unit_price,
    -- ~1% null, ~0.5% negative (invalid)
    CASE WHEN hash(i * 107) % 100 = 0 THEN NULL
         WHEN hash(i * 109) % 200 = 0 THEN -round((hash(i * 41) % 5000) / 100.0, 2)
         ELSE round((1 + (hash(i * 37) % 5)) * (1 + (hash(i * 41) % 30000) / 100.0), 2)
    END                                                             AS amount,
    round((hash(i * 43) % 30) / 100.0, 2)                           AS discount_pct,
    round((hash(i * 47) % 20) / 100.0, 2)                           AS tax_pct,
    ['web', 'store', 'app', 'partner'][1 + (hash(i * 53) % 4)::INT]  AS channel,
    ['ios', 'android', 'desktop', 'tablet'][1 + (hash(i * 59) % 4)::INT] AS device,
    ['card', 'cash', 'wallet', 'transfer'][1 + (hash(i * 61) % 4)::INT]  AS payment_method,
    (hash(i * 67) % 2)::INT                                         AS used_promo,
    CASE WHEN hash(i * 67) % 2 = 1
         THEN 'PROMO-' || ((hash(i * 71) % 500))::VARCHAR ELSE NULL END AS promo_code,
    ['GBP', 'USD', 'EUR'][1 + (hash(i * 73) % 3)::INT]              AS currency,
    ['north', 'south', 'east', 'west'][1 + (hash(i * 79) % 4)::INT] AS region,
    md5(i::VARCHAR)                                                 AS session_ref,
    'Mozilla/5.0 (build ' || ((hash(i * 83) % 900) + 100)::VARCHAR || '; rv:'
        || ((hash(i * 89) % 90) + 10)::VARCHAR || '.0) engine/'
        || ((hash(i * 97) % 9) + 1)::VARCHAR                        AS user_agent,
    '{{"os":"' || ['ios', 'android', 'desktop', 'tablet'][1 + (hash(i * 59) % 4)::INT]
        || '","app_version":' || ((hash(i * 113) % 40) + 1)::VARCHAR
        || ',"experiment":"' || ['A', 'B', 'C'][1 + (hash(i * 127) % 3)::INT]
        || '","score":' || round((hash(i * 131) % 1000) / 100.0, 2)::VARCHAR || '}}' AS attributes_json,
    repeat('note ', 8)                                              AS notes
FROM range({n_rows}) t(i)
"""


def build(n_rows, with_extras=False, force=False, log=print):
    """Build one family of files. Returns the path dict."""
    p = paths(n_rows)
    n_cust = max(1_000, n_rows // 20)
    n_prod = 5_000
    core_done = all(os.path.exists(p[k]) for k in ("sales", "customers", "products"))

    con = _con()
    if force or not core_done:
        # inconsistent country text on purpose
        con.execute(f"""COPY (
            SELECT i AS customer_id,
                   CASE hash(i * 13) % 10
                     WHEN 0 THEN lower(['UK','US','DE','FR','IN','BR','JP','AU'][1 + (hash(i*11) % 8)::INT])
                     WHEN 1 THEN ' ' || ['UK','US','DE','FR','IN','BR','JP','AU'][1 + (hash(i*11) % 8)::INT] || ' '
                     WHEN 2 THEN upper(substr(['UK','US','DE','FR','IN','BR','JP','AU'][1 + (hash(i*11) % 8)::INT], 1, 1))
                                 || lower(substr(['UK','US','DE','FR','IN','BR','JP','AU'][1 + (hash(i*11) % 8)::INT], 2))
                     ELSE ['UK','US','DE','FR','IN','BR','JP','AU'][1 + (hash(i*11) % 8)::INT]
                   END                                                   AS country,
                   ['retail','business','enterprise'][1 + (hash(i*17) % 3)::INT] AS segment,
                   DATE '2019-01-01' + ((hash(i*23) % 2000)::INT)        AS signup_date,
                   CASE WHEN hash(i*29) % 25 = 0 THEN NULL
                        ELSE 'cust' || i::VARCHAR || '@example.com' END  AS email
            FROM range({n_cust}) t(i)
        ) TO '{p["customers"]}' (FORMAT PARQUET)""")

        con.execute(f"""COPY (
            SELECT i AS product_id,
                   ['electronics','clothing','grocery','home','sports','books'][1 + (hash(i*7) % 6)::INT] AS category,
                   ['acme','globex','initech','umbrella'][1 + (hash(i*19) % 4)::INT] AS brand,
                   round(1 + (hash(i*13) % 50000) / 100.0, 2)           AS unit_cost
            FROM range({n_prod}) t(i)
        ) TO '{p["products"]}' (FORMAT PARQUET)""")

        # ~0.5% exact duplicate rows appended
        sel = _SALES_SELECT.format(n_rows=n_rows, n_cust=n_cust, n_prod=n_prod)
        dup_sel = _SALES_SELECT.format(n_rows=max(1, n_rows // 200), n_cust=n_cust, n_prod=n_prod)
        con.execute(f"COPY ({sel} UNION ALL {dup_sel}) TO '{p['sales']}' (FORMAT PARQUET)")
        log(f"   built sales/customers/products for {C.human(n_rows)} rows")

    if with_extras:
        _build_extras(con, p, n_rows, n_cust, force=force, log=log)

    con.close()
    return p


def _build_extras(con, p, n_rows, n_cust, force=False, log=print):
    """Second large table, a changed snapshot, a partitioned copy, a
    many-small-files copy and a CSV extract."""
    n_ret = max(1000, n_rows // 2)

    # Large on both sides, so joins against sales are a real shuffle.
    if force or not os.path.exists(p["returns"]):
        con.execute(f"""COPY (
            SELECT i                                              AS return_id,
                   (hash(i * 15485863) % {n_rows})::BIGINT        AS sale_id,
                   DATE '2024-01-05' + ((hash(i*3) % 545)::INT)   AS return_date,
                   round((hash(i*5) % 20000) / 100.0, 2)          AS refund_amount,
                   ['damaged','wrong item','changed mind','late'][1 + (hash(i*7) % 4)::INT] AS reason
            FROM range({n_ret}) t(i)
        ) TO '{p["returns"]}' (FORMAT PARQUET)""")
        log("   built returns (second large table for big-to-big joins)")

    # A later snapshot: some rows changed, some deleted.
    if force or not os.path.exists(p["sales_v2"]):
        con.execute(f"""COPY (
            SELECT sale_id,
                   CASE WHEN hash(sale_id * 3) % 20 = 0
                        THEN round(COALESCE(amount, 0) * 1.15, 2) ELSE amount END AS amount,
                   CASE WHEN hash(sale_id * 3) % 20 = 0 THEN 'amended' ELSE 'final' END AS status
            FROM read_parquet('{p["sales"]}')
            WHERE hash(sale_id * 7) % 50 <> 0            -- ~2% deleted
        ) TO '{p["sales_v2"]}' (FORMAT PARQUET)""")
        log("   built sales_v2 (later snapshot: changes, deletions)")

    if force or not os.path.isdir(p["sales_part"]):
        con.execute(f"""COPY (
            SELECT sale_id, customer_id, product_id, amount, quantity, channel, region,
                   date_trunc('month', sale_date)::DATE AS sale_month
            FROM read_parquet('{p["sales"]}')
        ) TO '{p["sales_part"]}'
          (FORMAT PARQUET, PARTITION_BY (sale_month), OVERWRITE_OR_IGNORE 1)""")
        log("   built partitioned copy (by month)")

    if force or not os.path.isdir(p["sales_small_files"]):
        os.makedirs(p["sales_small_files"], exist_ok=True)
        con.execute(f"""COPY (
            SELECT sale_id, customer_id, amount, channel,
                   (hash(sale_id) % 200)::INT AS shard
            FROM read_parquet('{p["sales"]}')
            WHERE hash(sale_id * 11) % 5 = 0
        ) TO '{p["sales_small_files"]}'
          (FORMAT PARQUET, PARTITION_BY (shard), OVERWRITE_OR_IGNORE 1)""")
        log("   built many-small-files copy (200 shards)")

    if force or not os.path.exists(p["sales_csv"]):
        n_csv = min(n_rows, 1_000_000)
        con.execute(f"""COPY (
            SELECT sale_id, customer_id, product_id, sale_date, quantity,
                   amount, channel, region, currency
            FROM read_parquet('{p["sales"]}') LIMIT {n_csv}
        ) TO '{p["sales_csv"]}' (FORMAT CSV, HEADER)""")
        log(f"   built CSV extract ({C.human(n_csv)} rows)")


def disk_report():
    rows = []
    for name in sorted(os.listdir(C.DATA_DIR)):
        full = os.path.join(C.DATA_DIR, name)
        if os.path.isdir(full):
            size = sum(os.path.getsize(os.path.join(dp, f))
                       for dp, _, fs in os.walk(full) for f in fs)
            kind = "folder"
        else:
            size = os.path.getsize(full)
            kind = "file"
        rows.append({"dataset": name, "kind": kind, "MB": round(size / 1024**2, 1)})
    return rows
