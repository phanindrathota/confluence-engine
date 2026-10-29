"""DuckDB views over bronze and silver. Nothing is copied; views read files in place.

Raw silver tables keep every observation (a 1m bucket fetched five times has
five rows, each with its own received_ts). That is what point-in-time replay
needs. The *_latest / *_first views are convenience projections.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from .storage.silver import SCHEMAS

VIEWS = {
    # A print first seen at its earliest received_ts: that is when we could have acted on it.
    "flow_print_first": """
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY id ORDER BY received_ts) AS rn FROM flow_print)
        WHERE rn = 1""",
    # Final value per bucket (latest observation). Use only for after-the-fact analysis.
    "net_drift_latest": """
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY ticker, event_ts, bucket ORDER BY received_ts DESC) AS rn
          FROM net_drift) WHERE rn = 1""",
    "bar_latest": """
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY ticker, event_ts, bucket ORDER BY received_ts DESC) AS rn
          FROM bar) WHERE rn = 1""",
    "news_first": """
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY id ORDER BY received_ts) AS rn FROM news_item)
        WHERE rn = 1""",
}


def connect(data_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    silver = data_dir / "silver"
    for table in SCHEMAS:
        files = list((silver / table).glob("*/*.parquet"))
        if files:
            glob = str(silver / table / "*" / "*.parquet")
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{glob}', "
                        f"hive_partitioning=false, union_by_name=true)")
        else:
            # Empty, correctly typed view so queries never fail on a fresh repo.
            cols = ", ".join(f"NULL::{_duck_type(f.type)} AS {f.name}" for f in SCHEMAS[table])
            con.execute(f"CREATE VIEW {table} AS SELECT {cols} WHERE false")
    for name, sql in VIEWS.items():
        con.execute(f"CREATE VIEW {name} AS {sql}")
    bronze = data_dir / "bronze" / "source=quantdata"
    if list(bronze.glob("*/*/*.jsonl.gz")):
        con.execute(f"CREATE VIEW bronze AS SELECT * FROM read_json_auto('{bronze}/*/*/*.jsonl.gz', "
                    f"format='newline_delimited', union_by_name=true, maximum_object_size=67108864)")
    return con


def _duck_type(t) -> str:
    import pyarrow as pa
    if pa.types.is_timestamp(t):
        return "TIMESTAMPTZ"
    if pa.types.is_date(t):
        return "DATE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_integer(t):
        return "BIGINT"
    if pa.types.is_floating(t):
        return "DOUBLE"
    if pa.types.is_list(t):
        return "VARCHAR[]"
    return "VARCHAR"
