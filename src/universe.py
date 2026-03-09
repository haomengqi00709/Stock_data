"""
Ticker universe builder.

Two modes:
  - "index"    : Nasdaq 100 + S&P 500 from Wikipedia (~517 tickers)
  - "exchange" : Full NASDAQ + NYSE from NASDAQ trader files,
                 filtered by market cap (default $500M+)

Usage:
    from src.universe import get_universe
    df = get_universe()                          # index mode (default)
    df = get_universe(mode="exchange")           # all NASDAQ+NYSE, $500M+ mcap
    df = get_universe(mode="exchange", min_market_cap=1e9)  # $1B+ filter
"""

import io
import json
import random
import time
from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
INDEX_CACHE    = RAW_DIR / "universe_index.json"
EXCHANGE_CACHE = RAW_DIR / "universe_exchange.json"

NASDAQ100_URL    = "https://en.wikipedia.org/wiki/Nasdaq-100"
SP500_URL        = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ_SCREEN    = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nasdaq"
NYSE_SCREEN      = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nyse"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Suffixes that indicate non-common-stock securities to drop
_DROP_SUFFIXES = (
    " ETF", " FUND", " TRUST", " NOTE", " UNIT", " WARRANT",
    " RIGHT", " PREFERRED", " ACQUISITION", " SPAC",
)
_DROP_TICKER_PATTERNS = (
    r"\^",   # index symbols
    r"=",    # futures/options
    r"\+$",  # preferred shares (e.g. BAC+)
    r"\.WS", # warrants
    r"\.U$", # units
    r"\.RT", # rights
)


# ── Index mode (Nasdaq 100 + S&P 500) ────────────────────────────────────────

def _fetch_wiki_table(url: str, ticker_col: str, name_col: str, label: str) -> pd.DataFrame:
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    for table in pd.read_html(io.StringIO(resp.text)):
        if ticker_col in table.columns:
            df = table[[ticker_col, name_col]].copy()
            df.columns = ["ticker", "name"]
            df["exchange"] = label
            return df
    raise ValueError(f"Column '{ticker_col}' not found at {url}")


def _get_index_universe() -> pd.DataFrame:
    print("[universe] Fetching Nasdaq 100 from Wikipedia...")
    ndx = _fetch_wiki_table(NASDAQ100_URL, "Ticker", "Company", "NASDAQ100")
    time.sleep(1)

    print("[universe] Fetching S&P 500 from Wikipedia...")
    sp500 = _fetch_wiki_table(SP500_URL, "Symbol", "Security", "SP500")

    combined = pd.concat([ndx, sp500], ignore_index=True)
    combined["ticker"] = combined["ticker"].str.replace(".", "-", regex=False).str.strip()

    exchange_map = (
        combined.groupby("ticker")["exchange"]
        .apply(lambda x: ",".join(sorted(set(x))))
        .reset_index()
    )
    deduped = (
        combined.drop_duplicates(subset="ticker")
        .drop(columns="exchange")
        .merge(exchange_map, on="ticker")
    )
    return deduped


# ── Exchange mode (full NASDAQ + NYSE) ───────────────────────────────────────

def _fetch_screener(url: str, exchange: str) -> pd.DataFrame:
    """Fetch one exchange from the NASDAQ screener API."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    rows = resp.json()["data"]["table"]["rows"]
    df = pd.DataFrame(rows)
    df = df.rename(columns={"symbol": "ticker", "name": "name", "marketCap": "market_cap"})
    df["exchange"] = exchange
    return df[["ticker", "name", "exchange", "market_cap"]].copy()


def _parse_market_cap(val) -> float:
    """Convert NASDAQ screener market cap string (e.g. '$1.23B') to float."""
    if not val or val in ("", "N/A"):
        return 0.0
    val = str(val).replace("$", "").replace(",", "").strip()
    if val.endswith("T"):
        return float(val[:-1]) * 1e12
    if val.endswith("B"):
        return float(val[:-1]) * 1e9
    if val.endswith("M"):
        return float(val[:-1]) * 1e6
    try:
        return float(val)
    except ValueError:
        return 0.0


def _fetch_exchange_list() -> pd.DataFrame:
    """Download all NASDAQ + NYSE stocks with market caps from the NASDAQ screener API."""
    print("[universe] Fetching NASDAQ stocks from screener...")
    nasdaq_df = _fetch_screener(NASDAQ_SCREEN, "NASDAQ")
    time.sleep(1)

    print("[universe] Fetching NYSE stocks from screener...")
    nyse_df = _fetch_screener(NYSE_SCREEN, "NYSE")

    combined = pd.concat([nasdaq_df, nyse_df], ignore_index=True)

    # Parse market cap to float
    combined["market_cap"] = combined["market_cap"].apply(_parse_market_cap)

    # Normalize tickers
    combined["ticker"] = combined["ticker"].str.strip().str.replace(".", "-", regex=False)

    # Drop non-common-stock names (warrants, units, preferred, etc.)
    name_upper = combined["name"].str.upper().fillna("")
    name_mask = ~name_upper.str.contains("|".join(s.strip() for s in _DROP_SUFFIXES))
    combined = combined[name_mask]

    # Drop tickers with special characters indicating non-stocks
    pattern = "|".join(_DROP_TICKER_PATTERNS)
    ticker_mask = ~combined["ticker"].str.contains(pattern, regex=True, na=False)
    combined = combined[ticker_mask]

    combined = combined.drop_duplicates(subset="ticker").reset_index(drop=True)
    print(f"[universe] Raw exchange list: {len(combined)} securities after basic filters")
    return combined


# ── Public API ────────────────────────────────────────────────────────────────

def get_universe(
    mode: str = "index",
    min_market_cap: float = 500e6,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Returns a deduplicated DataFrame with columns: ticker, name, exchange, [market_cap].

    Args:
        mode: "index" (Nasdaq100+SP500) or "exchange" (full NASDAQ+NYSE filtered by mcap)
        min_market_cap: minimum market cap in USD (only used in exchange mode)
        force_refresh: ignore cache and re-fetch
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if mode == "index":
        cache = INDEX_CACHE
        if cache.exists() and not force_refresh:
            print("[universe] Loading index universe from cache...")
            return pd.DataFrame(json.loads(cache.read_text()))
        df = _get_index_universe()
        cache.write_text(json.dumps(df.to_dict(orient="records"), indent=2))
        print(f"[universe] Index universe: {len(df)} tickers")
        return df

    elif mode == "exchange":
        cache = EXCHANGE_CACHE
        if cache.exists() and not force_refresh:
            print("[universe] Loading exchange universe from cache...")
            df = pd.DataFrame(json.loads(cache.read_text()))
            filtered = df[df["market_cap"] >= min_market_cap].copy()
            print(f"[universe] {len(filtered)} tickers with market cap >= ${min_market_cap/1e6:.0f}M")
            return filtered

        raw = _fetch_exchange_list()
        # Cache the full list (so re-filtering with different threshold is instant)
        raw_with_caps = raw[raw["market_cap"] > 0].copy()
        cache.write_text(json.dumps(raw_with_caps.to_dict(orient="records"), indent=2))

        filtered = raw_with_caps[raw_with_caps["market_cap"] >= min_market_cap].copy()
        filtered = filtered.sort_values("market_cap", ascending=False).reset_index(drop=True)
        print(f"[universe] Exchange universe: {len(filtered)} tickers >= ${min_market_cap/1e6:.0f}M market cap")
        return filtered

    else:
        raise ValueError(f"Unknown mode '{mode}'. Use 'index' or 'exchange'.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["index", "exchange"], default="index")
    parser.add_argument("--min-mcap", type=float, default=500e6)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    df = get_universe(mode=args.mode, min_market_cap=args.min_mcap, force_refresh=args.refresh)
    print(df.head(20).to_string())
    print(f"\nTotal: {len(df)} tickers")
