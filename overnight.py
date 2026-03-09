"""
Overnight data extraction pipeline.

1. Fetches full NASDAQ + NYSE universe (~2,800 tickers, $500M+ market cap)
2. Runs a 10-ticker smoke test first — aborts if any fail
3. On success, runs the full pipeline (skips already-downloaded tickers)
4. Logs everything to logs/overnight.log

Usage:
    python3 overnight.py
    python3 overnight.py --min-mcap 1000000000   # $1B+ filter
    python3 overnight.py --skip-test              # skip smoke test, go straight to full run
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Logging setup (file + stdout) ─────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"overnight_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger()

sys.path.insert(0, str(Path(__file__).parent))

from src.universe import get_universe
from src.prices import fetch_prices, fetch_dividends_and_splits
from src.fundamentals import fetch_fundamentals
from src.extras import fetch_extras
from src.macro import fetch_all_macro
from config import DATA_PROCESSED

# 10 test tickers: mix of large/mid cap, NASDAQ + NYSE, different sectors
TEST_TICKERS = ["AAPL", "JPM", "XOM", "JNJ", "TSLA", "V", "COST", "UNH", "NFLX", "CAT"]


def normalize_tickers(tickers: list[str]) -> list[str]:
    """Convert screener format to yfinance format (BRK/B → BRK-B)."""
    return [t.replace("/", "-") for t in tickers]


def validate_prices(tickers: list[str]) -> tuple[list[str], list[str]]:
    """Check which tickers have non-empty price files. Returns (ok, failed)."""
    import pandas as pd
    ok, failed = [], []
    prices_dir = DATA_PROCESSED / "prices"
    for t in tickers:
        path = prices_dir / f"{t}.parquet"
        if not path.exists():
            failed.append(f"{t} (file missing)")
            continue
        df = pd.read_parquet(path)
        if len(df) < 10:
            failed.append(f"{t} (only {len(df)} rows)")
        else:
            ok.append(t)
    return ok, failed


def validate_fundamentals(tickers: list[str]) -> tuple[list[str], list[str]]:
    """Check which tickers have non-empty income statement files."""
    import pandas as pd
    ok, failed = [], []
    fund_dir = DATA_PROCESSED / "fundamentals" / "income_annual"
    for t in tickers:
        path = fund_dir / f"{t}.parquet"
        if not path.exists():
            failed.append(f"{t} (file missing)")
            continue
        df = pd.read_parquet(path)
        if df.empty:
            failed.append(f"{t} (empty)")
        else:
            ok.append(t)
    return ok, failed


def validate_extras(tickers: list[str]) -> tuple[list[str], list[str]]:
    """Check that extras metrics snapshot exists and has data for test tickers."""
    import pandas as pd
    metrics_path = DATA_PROCESSED / "extras" / "metrics_extra.parquet"
    if not metrics_path.exists():
        return [], [f"{t} (metrics_extra.parquet missing)" for t in tickers]
    df = pd.read_parquet(metrics_path)
    ok, failed = [], []
    for t in tickers:
        if t in df.index:
            ok.append(t)
        else:
            failed.append(f"{t} (not in metrics_extra)")
    return ok, failed


def validate_macro() -> tuple[list[str], list[str]]:
    """Check that key macro files exist and have rows."""
    import pandas as pd
    required = ["market_vix", "market_sp500", "fred_fed_funds_rate", "fred_yield_spread_10_2"]
    ok, failed = [], []
    for name in required:
        path = DATA_PROCESSED / "macro" / f"{name}.parquet"
        if not path.exists():
            failed.append(f"{name} (missing)")
            continue
        df = pd.read_parquet(path)
        if len(df) < 10:
            failed.append(f"{name} (only {len(df)} rows)")
        else:
            ok.append(name)
    return ok, failed


def run_smoke_test() -> bool:
    log.info("=" * 60)
    log.info("SMOKE TEST — 10 tickers")
    log.info("=" * 60)
    log.info(f"Tickers: {TEST_TICKERS}")
    all_passed = True

    # Prices
    log.info(">> [1/5] Prices...")
    fetch_prices(TEST_TICKERS, years=10, skip_existing=False)
    price_ok, price_fail = validate_prices(TEST_TICKERS)
    log.info(f"   OK: {price_ok}")
    if price_fail:
        log.error(f"   FAILED: {price_fail}")
        all_passed = False

    # Dividends & splits
    log.info(">> [2/5] Dividends & splits...")
    fetch_dividends_and_splits(TEST_TICKERS, skip_existing=False)
    divs_ok = [t for t in TEST_TICKERS
               if (DATA_PROCESSED / "dividends" / f"{t}.parquet").exists()]
    log.info(f"   Dividend files found: {len(divs_ok)}/{len(TEST_TICKERS)}")

    # Fundamentals
    log.info(">> [3/5] Fundamentals...")
    fetch_fundamentals(TEST_TICKERS, skip_existing=False)
    fund_ok, fund_fail = validate_fundamentals(TEST_TICKERS)
    log.info(f"   OK: {fund_ok}")
    if fund_fail:
        log.warning(f"   WARNING (some tickers may have no filings): {fund_fail}")
        # Not a hard failure — some legit tickers lack yfinance fundamentals

    # Extras
    log.info(">> [4/5] Extras (analyst, holders, insider, options)...")
    fetch_extras(TEST_TICKERS, skip_existing=False)
    extra_ok, extra_fail = validate_extras(TEST_TICKERS)
    log.info(f"   OK: {extra_ok}")
    if extra_fail:
        log.warning(f"   WARNING: {extra_fail}")

    # Macro
    log.info(">> [5/5] Macro data...")
    fetch_all_macro()
    macro_ok, macro_fail = validate_macro()
    log.info(f"   OK: {macro_ok}")
    if macro_fail:
        log.error(f"   FAILED: {macro_fail}")
        all_passed = False

    if all_passed:
        log.info("SMOKE TEST PASSED ✓")
    else:
        log.error("SMOKE TEST FAILED ✗ — check errors above")
    return all_passed


def run_full_pipeline(tickers: list[str]):
    total = len(tickers)
    log.info("=" * 60)
    log.info(f"FULL PIPELINE — {total} tickers")
    log.info("=" * 60)
    start = time.time()

    t0 = time.time()

    log.info(">> Step 1/5: Prices (skipping existing)...")
    fetch_prices(tickers, years=10, skip_existing=True)
    log.info(f"   Done in {(time.time() - t0) / 60:.1f} min")

    t0 = time.time()
    log.info(">> Step 2/5: Dividends & splits (skipping existing)...")
    fetch_dividends_and_splits(tickers, skip_existing=True)
    log.info(f"   Done in {(time.time() - t0) / 60:.1f} min")

    t0 = time.time()
    log.info(">> Step 3/5: Fundamentals — income, balance sheet, cashflow (skipping existing)...")
    fetch_fundamentals(tickers, skip_existing=True)
    log.info(f"   Done in {(time.time() - t0) / 60:.1f} min")

    t0 = time.time()
    log.info(">> Step 4/5: Extras — analyst estimates, holders, insider, options (skipping existing)...")
    fetch_extras(tickers, skip_existing=True)
    log.info(f"   Done in {(time.time() - t0) / 60:.1f} min")

    log.info(">> Step 5/5: Macro data (one-time, already cached if re-running)...")
    fetch_all_macro()

    total_elapsed = (time.time() - start) / 60
    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE in {total_elapsed:.1f} minutes")
    log.info(f"Log saved to: {LOG_FILE}")
    log.info("=" * 60)

    # Final summary
    def count(path): return len(list(path.glob("*.parquet"))) if path.exists() else 0
    log.info("=== Final file counts ===")
    log.info(f"  Prices:       {count(DATA_PROCESSED / 'prices')}")
    log.info(f"  Dividends:    {count(DATA_PROCESSED / 'dividends')}")
    log.info(f"  Splits:       {count(DATA_PROCESSED / 'splits')}")
    log.info(f"  Fundamentals: {count(DATA_PROCESSED / 'fundamentals' / 'income_annual')}")
    log.info(f"  Extras:       {count(DATA_PROCESSED / 'extras' / 'earnings_dates')}")
    log.info(f"  Macro:        {count(DATA_PROCESSED / 'macro')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-mcap", type=float, default=500e6,
                        help="Minimum market cap in USD (default: $500M)")
    parser.add_argument("--skip-test", action="store_true",
                        help="Skip smoke test and go straight to full run")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("OVERNIGHT DATA PIPELINE STARTED")
    log.info(f"Min market cap: ${args.min_mcap/1e6:.0f}M")
    log.info("=" * 60)

    # Get universe
    log.info(">> Fetching ticker universe...")
    universe = get_universe(mode="exchange", min_market_cap=args.min_mcap)
    tickers = normalize_tickers(universe["ticker"].tolist())
    log.info(f"Universe: {len(tickers)} tickers")

    # Smoke test
    if not args.skip_test:
        passed = run_smoke_test()
        if not passed:
            log.error("Smoke test failed — aborting full run. Check logs above.")
            sys.exit(1)
        log.info("Smoke test passed. Starting full pipeline in 5 seconds...")
        time.sleep(5)
    else:
        log.info("Smoke test skipped.")

    # Full run
    run_full_pipeline(tickers)


if __name__ == "__main__":
    main()
