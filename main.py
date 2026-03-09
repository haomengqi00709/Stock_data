"""
Main pipeline orchestrator.
Run this to do a full cold-start data pull for Nasdaq 100 + S&P 500.

Usage:
    python main.py                  # Full run
    python main.py --universe-only  # Just refresh ticker list
    python main.py --prices-only    # Only price data
    python main.py --fundamentals-only
    python main.py --validate       # Run data quality checks
"""

import argparse
import sys
import time

from config import DATA_PROCESSED
from src.universe import get_universe
from src.prices import fetch_prices, fetch_dividends_and_splits
from src.fundamentals import fetch_fundamentals
from src.storage import get_connection, create_views, validate_prices


def run_pipeline(args):
    start = time.time()

    # --- Universe ---
    print("\n=== Step 1: Building ticker universe ===")
    universe = get_universe(force_refresh=args.refresh_universe)
    tickers = universe["ticker"].tolist()
    print(f"Universe: {len(tickers)} tickers")

    if args.universe_only:
        print(universe.to_string())
        return

    # --- Prices ---
    if not args.fundamentals_only and not args.validate:
        print("\n=== Step 2: Fetching price data ===")
        fetch_prices(tickers, skip_existing=not args.no_skip)
        print("\n=== Step 3: Fetching dividends & splits ===")
        fetch_dividends_and_splits(tickers, skip_existing=not args.no_skip)

    # --- Fundamentals ---
    if not args.prices_only and not args.validate:
        print("\n=== Step 4: Fetching fundamental data ===")
        fetch_fundamentals(tickers, skip_existing=not args.no_skip)

    # --- Validate ---
    if not args.prices_only and not args.fundamentals_only:
        print("\n=== Step 5: Data validation ===")
        con = get_connection()
        create_views(con)

        suspicious = validate_prices(con)
        if suspicious.empty:
            print("[validate] No suspicious price jumps found.")
        else:
            print(f"[validate] Found {len(suspicious)} suspicious rows — check for unadjusted splits:")
            print(suspicious.to_string())

    elapsed = time.time() - start
    print(f"\n=== Pipeline complete in {elapsed/60:.1f} minutes ===")


def main():
    parser = argparse.ArgumentParser(description="Autotrader2 data pipeline")
    parser.add_argument("--refresh-universe", action="store_true", help="Force re-fetch ticker list")
    parser.add_argument("--universe-only", action="store_true")
    parser.add_argument("--prices-only", action="store_true")
    parser.add_argument("--fundamentals-only", action="store_true")
    parser.add_argument("--validate", action="store_true", help="Only run data validation")
    parser.add_argument("--no-skip", action="store_true", help="Re-download even if file exists")
    args = parser.parse_args()

    run_pipeline(args)


if __name__ == "__main__":
    main()
