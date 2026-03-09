"""
Quick inspection of the combined price Parquet file.

Usage:
    python3 check_combined.py              # show top 10 rows
    python3 check_combined.py --tail       # show last 10 rows
    python3 check_combined.py --ticker AAPL
    python3 check_combined.py --ticker AAPL --tail
"""

import argparse

import duckdb

from config import DATA_PROCESSED

PATH = str(DATA_PROCESSED / "prices_combined.parquet")


def run(ticker: str | None, tail: bool):
    con = duckdb.connect()

    # Summary stats
    summary = con.execute(f"""
        SELECT
            COUNT(*)            AS total_rows,
            COUNT(DISTINCT ticker) AS tickers,
            MIN(Date)::DATE     AS earliest,
            MAX(Date)::DATE     AS latest,
            COUNT(DISTINCT Date::DATE) AS trading_days
        FROM '{PATH}'
    """).df()
    print("=== Combined File Summary ===")
    print(summary.to_string(index=False))

    # Per-ticker or all
    if ticker:
        where = f"WHERE ticker = '{ticker.upper()}'"
        label = ticker.upper()
    else:
        where = ""
        label = "all tickers"

    order = "DESC" if tail else "ASC"
    direction = "Last" if tail else "Top"

    print(f"\n=== {direction} 10 rows — {label} ===")
    rows = con.execute(f"""
        SELECT Date::DATE AS date, ticker, Open, High, Low, Close, Volume
        FROM '{PATH}'
        {where}
        ORDER BY ticker ASC, Date {order}
        LIMIT 10
    """).df()
    print(rows.to_string(index=False))

    # Per-ticker row counts (top 10 most / least data)
    if not ticker:
        print("\n=== Tickers with least data (possible new listings) ===")
        least = con.execute(f"""
            SELECT ticker, COUNT(*) AS rows, MIN(Date)::DATE AS from_, MAX(Date)::DATE AS to_
            FROM '{PATH}'
            GROUP BY ticker
            ORDER BY rows ASC
            LIMIT 10
        """).df()
        print(least.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", type=str, default=None, help="Filter to a single ticker")
    parser.add_argument("--tail", action="store_true", help="Show last 10 rows instead of first")
    args = parser.parse_args()
    run(args.ticker, args.tail)
