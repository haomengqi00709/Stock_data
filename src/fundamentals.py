"""
Download financial statements (income, balance sheet, cash flow) and
key metrics (PE, PB, ROE, etc.) via yfinance.
Stores per-ticker Parquet files under data/processed/fundamentals/.
"""

import random
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

from config import DATA_PROCESSED, SLEEP_MIN, SLEEP_MAX, BATCH_SIZE, MAX_RETRIES

FUND_DIR = DATA_PROCESSED / "fundamentals"


def _save_parquet(df: pd.DataFrame, path: Path):
    if df is None or df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # Transpose so rows = dates, cols = line items (easier to query)
    df = df.T.copy()
    df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    df.index.name = "period_end"
    df.to_parquet(path, index=True, engine="pyarrow", compression="snappy")


def _fetch_statements(ticker_obj: yf.Ticker, ticker: str):
    """Fetch and save all four financial statement types."""
    statements = {
        "income_annual": ticker_obj.income_stmt,
        "income_quarterly": ticker_obj.quarterly_income_stmt,
        "balance_annual": ticker_obj.balance_sheet,
        "balance_quarterly": ticker_obj.quarterly_balance_sheet,
        "cashflow_annual": ticker_obj.cashflow,
        "cashflow_quarterly": ticker_obj.quarterly_cashflow,
    }
    for name, df in statements.items():
        path = FUND_DIR / name / f"{ticker}.parquet"
        try:
            _save_parquet(df, path)
        except Exception as e:
            print(f"  [fundamentals] {ticker}/{name} error: {e}")


def _fetch_metrics(ticker_obj: yf.Ticker, ticker: str) -> dict | None:
    """
    Extract key valuation and quality metrics from yfinance .info dict.
    Returns a flat dict or None on failure.
    """
    try:
        info = ticker_obj.info
    except Exception as e:
        print(f"  [metrics] {ticker} info error: {e}")
        return None

    fields = {
        "ticker": ticker,
        "name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "market_cap": info.get("marketCap"),
        "pe_trailing": info.get("trailingPE"),
        "pe_forward": info.get("forwardPE"),
        "pb": info.get("priceToBook"),
        "ps": info.get("priceToSalesTrailing12Months"),
        "ev_ebitda": info.get("enterpriseToEbitda"),
        "roe": info.get("returnOnEquity"),
        "roa": info.get("returnOnAssets"),
        "profit_margin": info.get("profitMargins"),
        "operating_margin": info.get("operatingMargins"),
        "revenue_growth": info.get("revenueGrowth"),
        "earnings_growth": info.get("earningsGrowth"),
        "debt_to_equity": info.get("debtToEquity"),
        "current_ratio": info.get("currentRatio"),
        "quick_ratio": info.get("quickRatio"),
        "dividend_yield": info.get("dividendYield"),
        "payout_ratio": info.get("payoutRatio"),
        "beta": info.get("beta"),
        "52w_high": info.get("fiftyTwoWeekHigh"),
        "52w_low": info.get("fiftyTwoWeekLow"),
        "shares_outstanding": info.get("sharesOutstanding"),
        "float_shares": info.get("floatShares"),
        "short_ratio": info.get("shortRatio"),
    }
    return fields


def fetch_fundamentals(tickers: list[str], skip_existing: bool = True):
    """
    Download financial statements and metrics for all tickers.
    """
    FUND_DIR.mkdir(parents=True, exist_ok=True)
    metrics_path = FUND_DIR / "metrics_snapshot.parquet"

    if skip_existing:
        # Skip tickers that already have income statement data
        tickers = [
            t for t in tickers
            if not (FUND_DIR / "income_annual" / f"{t}.parquet").exists()
        ]
        print(f"[fundamentals] {len(tickers)} tickers to download (skipping existing)")

    if not tickers:
        print("[fundamentals] All tickers already downloaded.")
        return

    all_metrics = []

    for idx, ticker in enumerate(tickers):
        print(f"  [{idx+1}/{len(tickers)}] {ticker}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                t = yf.Ticker(ticker)
                _fetch_statements(t, ticker)
                metrics = _fetch_metrics(t, ticker)
                if metrics:
                    all_metrics.append(metrics)
                break
            except Exception as e:
                wait = 2 ** attempt + random.uniform(0, 1)
                print(f"  [retry {attempt}/{MAX_RETRIES}] {ticker}: {e} — waiting {wait:.1f}s")
                time.sleep(wait)

        if (idx + 1) % BATCH_SIZE == 0:
            sleep_secs = random.uniform(SLEEP_MIN, SLEEP_MAX)
            print(f"  [fundamentals] Sleeping {sleep_secs:.1f}s...")
            time.sleep(sleep_secs)

    # Save metrics snapshot
    if all_metrics:
        metrics_df = pd.DataFrame(all_metrics).set_index("ticker")
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_df.to_parquet(metrics_path, engine="pyarrow", compression="snappy")
        print(f"[fundamentals] Saved metrics snapshot: {len(metrics_df)} tickers")

    print("[fundamentals] Done.")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.universe import get_universe
    tickers = get_universe()["ticker"].tolist()
    fetch_fundamentals(tickers)
