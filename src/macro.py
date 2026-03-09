"""
Market-wide macro data from two sources:
  1. FRED API  — Fed funds rate, CPI, unemployment, GDP, yield curve, credit spreads
  2. yfinance  — VIX, 10Y/2Y Treasury yields, S&P 500, Dollar index

Stored as Parquet under data/processed/macro/

FRED API key is free: https://fred.stlouisfed.org/docs/api/api_key.html
Set FRED_API_KEY in your .env file.
Without a key, FRED series are skipped and only yfinance series are fetched.
"""

import time
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

from config import DATA_PROCESSED, FRED_API_KEY

MACRO_DIR = DATA_PROCESSED / "macro"

# ── FRED series to fetch ──────────────────────────────────────────────────────
FRED_SERIES = {
    "fed_funds_rate":    "DFF",        # Federal Funds Effective Rate (daily)
    "cpi":               "CPIAUCSL",   # CPI All Urban Consumers (monthly)
    "unemployment":      "UNRATE",     # Unemployment Rate (monthly)
    "gdp":               "GDP",        # Real GDP (quarterly)
    "treasury_10y":      "DGS10",      # 10-Year Treasury Yield (daily)
    "treasury_2y":       "DGS2",       # 2-Year Treasury Yield (daily)
    "yield_spread_10_2": "T10Y2Y",     # 10Y-2Y Spread — yield curve (daily)
    "yield_spread_10_3m":"T10Y3M",     # 10Y-3M Spread (daily)
    "hy_credit_spread":  "BAMLH0A0HYM2",  # High Yield OAS (daily)
    "ig_credit_spread":  "BAMLC0A0CM",    # Investment Grade OAS (daily)
    "m2_money_supply":   "M2SL",       # M2 Money Supply (monthly)
    "consumer_sentiment":"UMCSENT",    # U Michigan Consumer Sentiment (monthly)
}

# ── yfinance tickers for market indicators ────────────────────────────────────
YF_SERIES = {
    "vix":          "^VIX",    # CBOE Volatility Index
    "sp500":        "^GSPC",   # S&P 500
    "nasdaq":       "^IXIC",   # Nasdaq Composite
    "russell2000":  "^RUT",    # Russell 2000 (small cap)
    "dxy":          "DX-Y.NYB",# US Dollar Index
    "gold":         "GC=F",    # Gold futures
    "oil_wti":      "CL=F",    # WTI Crude Oil futures
    "treasury_10y": "^TNX",    # 10Y Treasury yield (redundant with FRED but faster)
}


def _save(df: pd.DataFrame, name: str):
    if df is None or df.empty:
        return
    MACRO_DIR.mkdir(parents=True, exist_ok=True)
    path = MACRO_DIR / f"{name}.parquet"
    df.index = pd.to_datetime(df.index, utc=True)
    df.to_parquet(path, engine="pyarrow", compression="snappy")
    print(f"  [macro] Saved {name}: {len(df)} rows")


def fetch_fred(years: int = 15):
    """Fetch all FRED macro series."""
    if not FRED_API_KEY or FRED_API_KEY == "your_fred_api_key_here":
        print("[macro] No FRED API key — skipping FRED series.")
        print("        Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        print("        Then add FRED_API_KEY=<key> to your .env file.")
        return

    print(f"[macro] Fetching {len(FRED_SERIES)} FRED series...")
    base_url = "https://api.stlouisfed.org/fred/series/observations"

    for name, series_id in FRED_SERIES.items():
        try:
            params = {
                "series_id":      series_id,
                "api_key":        FRED_API_KEY,
                "file_type":      "json",
                "observation_start": f"{2025 - years}-01-01",
            }
            resp = requests.get(base_url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()["observations"]

            df = pd.DataFrame(data)[["date", "value"]].copy()
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"]).set_index("date")
            df.columns = [name]
            _save(df, f"fred_{name}")
            time.sleep(0.2)  # FRED rate limit: 120 req/min

        except Exception as e:
            print(f"  [macro] FRED {series_id} error: {e}")

    print("[macro] FRED done.")


def fetch_market_indicators(years: int = 15):
    """Fetch market-wide indicators via yfinance."""
    print(f"[macro] Fetching {len(YF_SERIES)} market indicators via yfinance...")

    for name, ticker in YF_SERIES.items():
        try:
            df = yf.download(
                ticker,
                period=f"{years}y",
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            if df.empty:
                print(f"  [macro] No data for {ticker} ({name})")
                continue
            # Flatten MultiIndex if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df[["Close"]].copy()
            df.columns = [name]
            _save(df, f"market_{name}")
            time.sleep(0.5)

        except Exception as e:
            print(f"  [macro] {ticker} error: {e}")

    print("[macro] Market indicators done.")


def fetch_all_macro(years: int = 15):
    fetch_fred(years=years)
    fetch_market_indicators(years=years)


def load_macro_panel() -> pd.DataFrame:
    """
    Load all macro series into a single wide DataFrame (daily, forward-filled).
    Useful for merging with price data in strategy backtests.
    """
    files = list(MACRO_DIR.glob("*.parquet"))
    if not files:
        print("[macro] No macro data found. Run fetch_all_macro() first.")
        return pd.DataFrame()

    frames = []
    for f in files:
        df = pd.read_parquet(f)
        frames.append(df)

    panel = pd.concat(frames, axis=1).sort_index()
    panel = panel.resample("D").last().ffill()  # daily, forward-fill gaps
    return panel


if __name__ == "__main__":
    fetch_all_macro()
    panel = load_macro_panel()
    print(f"\nMacro panel: {panel.shape}")
    print(panel.tail(5).to_string())
