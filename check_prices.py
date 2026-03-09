"""
Scan all price Parquet files, report coverage, flag issues,
and optionally combine everything into one master Parquet file.

Usage:
    python3 check_prices.py              # just report
    python3 check_prices.py --combine    # report + save combined file
"""

import argparse
from pathlib import Path

import pandas as pd

from config import DATA_PROCESSED

PRICES_DIR = DATA_PROCESSED / "prices"
COMBINED_PATH = DATA_PROCESSED / "prices_combined.parquet"

# Anything with fewer rows than this gets flagged
MIN_EXPECTED_ROWS = 200  # ~1 year of trading days


def check_prices(combine: bool = False):
    files = sorted(PRICES_DIR.glob("*.parquet"))
    if not files:
        print("No price files found.")
        return

    print(f"Scanning {len(files)} files...\n")

    records = []
    frames = []
    issues = []

    for f in files:
        ticker = f.stem
        try:
            df = pd.read_parquet(f)
            # Flatten MultiIndex columns if yfinance stored them that way
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(-1)
        except Exception as e:
            issues.append({"ticker": ticker, "issue": f"unreadable: {e}", "rows": 0})
            continue

        rows = len(df)
        if rows == 0:
            issues.append({"ticker": ticker, "issue": "empty", "rows": 0})
            continue

        start = df.index.min()
        end = df.index.max()
        has_nulls = df[["Open", "High", "Low", "Close", "Volume"]].isnull().any().any()
        null_pct = df[["Open", "High", "Low", "Close"]].isnull().mean().mean() * 100

        record = {
            "ticker": ticker,
            "rows": rows,
            "start": start.date() if hasattr(start, "date") else start,
            "end": end.date() if hasattr(end, "date") else end,
            "null_%": round(null_pct, 2),
        }
        records.append(record)

        if rows < MIN_EXPECTED_ROWS:
            issues.append({"ticker": ticker, "issue": f"only {rows} rows", "rows": rows})
        elif has_nulls:
            issues.append({"ticker": ticker, "issue": f"{null_pct:.1f}% nulls", "rows": rows})

        if combine:
            frames.append(df)

    # Summary table
    summary = pd.DataFrame(records)
    print("=== Coverage Summary ===")
    print(f"Total tickers : {len(records)}")
    print(f"Avg rows      : {summary['rows'].mean():.0f}")
    print(f"Min rows      : {summary['rows'].min()} ({summary.loc[summary['rows'].idxmin(), 'ticker']})")
    print(f"Max rows      : {summary['rows'].max()} ({summary.loc[summary['rows'].idxmax(), 'ticker']})")
    print(f"Date range    : {summary['start'].min()} → {summary['end'].max()}")
    print(f"With nulls    : {(summary['null_%'] > 0).sum()} tickers")

    # Issues
    if issues:
        print(f"\n=== Issues Found ({len(issues)}) ===")
        issues_df = pd.DataFrame(issues)
        print(issues_df.to_string(index=False))
    else:
        print("\n=== No issues found ===")

    # Full table (sorted by rows ascending so sparse ones are at top)
    print("\n=== Full Coverage Table (sorted by row count) ===")
    print(summary.sort_values("rows").to_string(index=False))

    # Combine
    if combine:
        print(f"\nCombining {len(frames)} DataFrames...")
        combined = pd.concat(frames, ignore_index=False)
        # Drop ghost empty column left by yfinance MultiIndex
        combined = combined.loc[:, combined.columns.str.strip() != ""]
        combined = combined.sort_values(["ticker", combined.index.name or "Date"])
        combined.to_parquet(COMBINED_PATH, engine="pyarrow", compression="snappy")
        print(f"Saved combined file: {COMBINED_PATH}")
        print(f"Total rows: {len(combined):,} across {combined['ticker'].nunique()} tickers")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--combine", action="store_true", help="Also save a combined Parquet file")
    args = parser.parse_args()
    check_prices(combine=args.combine)
