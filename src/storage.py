"""
DuckDB helpers for querying the Parquet data lake.
All heavy lifting stays in Parquet files; DuckDB is a query layer on top.
"""

from pathlib import Path

import duckdb
import pandas as pd

from config import DATA_PROCESSED, DB_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def create_views(con: duckdb.DuckDBPyConnection = None):
    """
    Register Parquet directories as DuckDB views so you can query them
    without loading everything into memory.
    """
    if con is None:
        con = get_connection()

    views = {
        "prices": DATA_PROCESSED / "prices" / "*.parquet",
        "dividends": DATA_PROCESSED / "dividends" / "*.parquet",
        "splits": DATA_PROCESSED / "splits" / "*.parquet",
        "income_annual": DATA_PROCESSED / "fundamentals" / "income_annual" / "*.parquet",
        "income_quarterly": DATA_PROCESSED / "fundamentals" / "income_quarterly" / "*.parquet",
        "balance_annual": DATA_PROCESSED / "fundamentals" / "balance_annual" / "*.parquet",
        "balance_quarterly": DATA_PROCESSED / "fundamentals" / "balance_quarterly" / "*.parquet",
        "cashflow_annual": DATA_PROCESSED / "fundamentals" / "cashflow_annual" / "*.parquet",
        "cashflow_quarterly": DATA_PROCESSED / "fundamentals" / "cashflow_quarterly" / "*.parquet",
        "metrics": DATA_PROCESSED / "fundamentals" / "metrics_snapshot.parquet",
    }

    for name, path in views.items():
        if Path(str(path).replace("*.parquet", "")).exists():
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
            print(f"  [storage] View created: {name}")

    return con


def query(sql: str, con: duckdb.DuckDBPyConnection = None) -> pd.DataFrame:
    if con is None:
        con = get_connection()
        create_views(con)
    return con.execute(sql).df()


def validate_prices(con: duckdb.DuckDBPyConnection = None) -> pd.DataFrame:
    """
    Basic data quality check: find tickers with suspicious price jumps
    (>50% single-day move) which may indicate unadjusted split data.
    """
    sql = """
    SELECT
        ticker,
        date::DATE AS date,
        Close,
        LAG(Close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
        ROUND(ABS(Close / NULLIF(LAG(Close) OVER (PARTITION BY ticker ORDER BY date), 0) - 1), 4) AS pct_change
    FROM prices
    QUALIFY pct_change > 0.5
    ORDER BY pct_change DESC
    LIMIT 50
    """
    return query(sql, con)


def get_latest_metrics(con: duckdb.DuckDBPyConnection = None) -> pd.DataFrame:
    """Return the latest metrics snapshot sorted by market cap."""
    sql = """
    SELECT *
    FROM metrics
    ORDER BY market_cap DESC NULLS LAST
    """
    return query(sql, con)


if __name__ == "__main__":
    con = get_connection()
    create_views(con)
    print("\n=== Suspicious price jumps (potential split errors) ===")
    print(validate_prices(con).to_string())
