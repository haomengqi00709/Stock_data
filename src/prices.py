"""
Download OHLCV (adjusted), dividends, and splits for a list of tickers via yfinance.
Stores each ticker as a Parquet file under data/processed/prices/.
"""

import random
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

from config import DATA_PROCESSED, HISTORY_YEARS, SLEEP_MIN, SLEEP_MAX, BATCH_SIZE, MAX_RETRIES

PRICES_DIR = DATA_PROCESSED / "prices"
DIVS_DIR = DATA_PROCESSED / "dividends"
SPLITS_DIR = DATA_PROCESSED / "splits"


def _download_with_retry(ticker: str, period: str) -> yf.Ticker:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return yf.Ticker(ticker)
        except Exception as e:
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"  [retry {attempt}/{MAX_RETRIES}] {ticker}: {e} — waiting {wait:.1f}s")
            time.sleep(wait)
    return None


def _save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=True, engine="pyarrow", compression="snappy")


def fetch_prices(tickers: list[str], years: int = HISTORY_YEARS, skip_existing: bool = True):
    """
    Download adjusted OHLCV for all tickers.
    Uses yfinance bulk download in batches for efficiency.
    """
    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    if skip_existing:
        tickers = [t for t in tickers if not (PRICES_DIR / f"{t}.parquet").exists()]
        print(f"[prices] {len(tickers)} tickers to download (skipping existing)")

    if not tickers:
        print("[prices] All tickers already downloaded.")
        return

    period = f"{years}y"

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        print(f"[prices] Batch {i // BATCH_SIZE + 1}: {batch}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                raw = yf.download(
                    batch,
                    period=period,
                    auto_adjust=True,   # gives Adjusted OHLCV directly
                    actions=False,
                    group_by="ticker",
                    threads=False,      # avoid SQLite cache lock on concurrent access
                    progress=False,
                )
                break
            except Exception as e:
                wait = 2 ** attempt + random.uniform(0, 1)
                print(f"  [retry {attempt}/{MAX_RETRIES}] batch error: {e} — waiting {wait:.1f}s")
                time.sleep(wait)
        else:
            print(f"  [prices] Batch failed after {MAX_RETRIES} retries, skipping.")
            continue

        # Split multi-ticker download back into per-ticker DataFrames
        if len(batch) == 1:
            ticker = batch[0]
            df = raw.dropna(how="all").copy()
            if df.empty:
                print(f"  [prices] No data for {ticker}, skipping.")
            else:
                df.index = pd.to_datetime(df.index, utc=True)
                df["ticker"] = ticker
                _save_parquet(df, PRICES_DIR / f"{ticker}.parquet")
        else:
            for ticker in batch:
                try:
                    df = raw[ticker].dropna(how="all").copy()
                    if df.empty:
                        print(f"  [prices] No data for {ticker}, skipping.")
                        continue
                    df.index = pd.to_datetime(df.index, utc=True)
                    df["ticker"] = ticker
                    _save_parquet(df, PRICES_DIR / f"{ticker}.parquet")
                except KeyError:
                    print(f"  [prices] No data for {ticker}")

        sleep_secs = random.uniform(SLEEP_MIN, SLEEP_MAX)
        print(f"  [prices] Sleeping {sleep_secs:.1f}s...")
        time.sleep(sleep_secs)

    print("[prices] Done.")


def fetch_dividends_and_splits(tickers: list[str], skip_existing: bool = True):
    """
    Download dividend history and split history per ticker.
    """
    DIVS_DIR.mkdir(parents=True, exist_ok=True)
    SPLITS_DIR.mkdir(parents=True, exist_ok=True)

    if skip_existing:
        tickers = [
            t for t in tickers
            if not (DIVS_DIR / f"{t}.parquet").exists()
            or not (SPLITS_DIR / f"{t}.parquet").exists()
        ]

    print(f"[dividends/splits] {len(tickers)} tickers to process")

    for idx, ticker in enumerate(tickers):
        print(f"  [{idx+1}/{len(tickers)}] {ticker}")
        t = _download_with_retry(ticker, period="max")
        if t is None:
            continue

        try:
            divs = t.dividends
            if not divs.empty:
                divs.index = pd.to_datetime(divs.index, utc=True)
                divs.name = "dividend"
                _save_parquet(divs.to_frame(), DIVS_DIR / f"{ticker}.parquet")

            splits = t.splits
            if not splits.empty:
                splits.index = pd.to_datetime(splits.index, utc=True)
                splits.name = "split_ratio"
                _save_parquet(splits.to_frame(), SPLITS_DIR / f"{ticker}.parquet")
        except Exception as e:
            print(f"  [div/split] {ticker} error: {e}")

        if (idx + 1) % BATCH_SIZE == 0:
            sleep_secs = random.uniform(SLEEP_MIN, SLEEP_MAX)
            print(f"  [div/split] Sleeping {sleep_secs:.1f}s...")
            time.sleep(sleep_secs)

    print("[dividends/splits] Done.")


if __name__ == "__main__":
    from src.universe import get_universe
    tickers = get_universe()["ticker"].tolist()
    fetch_prices(tickers)
    fetch_dividends_and_splits(tickers)
