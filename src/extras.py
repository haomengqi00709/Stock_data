"""
Per-ticker supplementary data via yfinance:
  - Analyst estimates (EPS + revenue)
  - Institutional holders
  - Short interest
  - Insider transactions
  - Earnings dates

All stored as Parquet under data/processed/extras/<category>/<TICKER>.parquet
Metrics snapshot (short interest, etc.) stored in extras/metrics_extra.parquet
"""

import random
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

from config import DATA_PROCESSED, SLEEP_MIN, SLEEP_MAX, BATCH_SIZE, MAX_RETRIES

EXTRAS_DIR = DATA_PROCESSED / "extras"


def _save(df: pd.DataFrame | None, path: Path):
    if df is None or df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", compression="snappy")


def _get_with_retry(ticker: str) -> yf.Ticker | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return yf.Ticker(ticker)
        except Exception as e:
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"  [retry {attempt}/{MAX_RETRIES}] {ticker}: {e} — waiting {wait:.1f}s")
            time.sleep(wait)
    return None


def _fetch_analyst_estimates(t: yf.Ticker, ticker: str):
    """EPS and revenue estimates (quarterly + annual)."""
    datasets = {
        "eps_estimate":     getattr(t, "earnings_estimate", None),
        "revenue_estimate": getattr(t, "revenue_estimate", None),
        "eps_trend":        getattr(t, "eps_trend", None),
        "eps_revisions":    getattr(t, "eps_revisions", None),
        "growth_estimates": getattr(t, "growth_estimates", None),
    }
    for name, df in datasets.items():
        if df is not None and not df.empty:
            _save(df, EXTRAS_DIR / "analyst" / name / f"{ticker}.parquet")


def _fetch_holders(t: yf.Ticker, ticker: str):
    """Top institutional and mutual fund holders."""
    inst = getattr(t, "institutional_holders", None)
    mutual = getattr(t, "mutualfund_holders", None)
    _save(inst,   EXTRAS_DIR / "holders" / "institutional" / f"{ticker}.parquet")
    _save(mutual, EXTRAS_DIR / "holders" / "mutualfund"    / f"{ticker}.parquet")


def _fetch_insider(t: yf.Ticker, ticker: str):
    """Insider purchases and full transaction history."""
    purchases = getattr(t, "insider_purchases", None)
    transactions = getattr(t, "insider_transactions", None)
    _save(purchases,    EXTRAS_DIR / "insider" / "purchases"    / f"{ticker}.parquet")
    _save(transactions, EXTRAS_DIR / "insider" / "transactions" / f"{ticker}.parquet")


def _fetch_earnings_dates(t: yf.Ticker, ticker: str):
    """Past and upcoming earnings announcement dates."""
    try:
        df = t.earnings_dates
        _save(df, EXTRAS_DIR / "earnings_dates" / f"{ticker}.parquet")
    except Exception:
        pass


def _fetch_recommendations(t: yf.Ticker, ticker: str) -> dict:
    """
    Analyst consensus recommendations history + current summary.
    Returns a flat dict of the latest consensus for the metrics snapshot.
    """
    result = {"ticker": ticker, "rec_mean": None, "rec_key": None, "num_analysts": None}
    try:
        # Full history — each row is a period with strongBuy/buy/hold/sell counts
        recs = getattr(t, "recommendations", None)
        _save(recs, EXTRAS_DIR / "recommendations" / f"{ticker}.parquet")

        # Upgrades/downgrades event log
        upgrades = getattr(t, "upgrades_downgrades", None)
        _save(upgrades, EXTRAS_DIR / "upgrades_downgrades" / f"{ticker}.parquet")

        # Current consensus summary from .info
        info = t.info
        result["rec_mean"]     = info.get("recommendationMean")   # 1=Strong Buy, 5=Sell
        result["rec_key"]      = info.get("recommendationKey")    # e.g. "buy", "hold"
        result["num_analysts"] = info.get("numberOfAnalystOpinions")
    except Exception:
        pass
    return result


def _fetch_esg(t: yf.Ticker, ticker: str) -> dict:
    """
    ESG / sustainability scores from Sustainalytics via yfinance.
    Returns a flat dict for the metrics snapshot.
    """
    result = {
        "ticker": ticker,
        "esg_total": None, "esg_env": None,
        "esg_social": None, "esg_governance": None,
        "esg_controversy": None, "esg_rating": None,
    }
    try:
        sus = t.sustainability
        if sus is None or sus.empty:
            return result
        # sustainability is a single-column DataFrame with metric names as index
        data = sus.iloc[:, 0].to_dict()
        result["esg_total"]       = data.get("totalEsg")
        result["esg_env"]         = data.get("environmentScore")
        result["esg_social"]      = data.get("socialScore")
        result["esg_governance"]  = data.get("governanceScore")
        result["esg_controversy"] = data.get("highestControversy")
        result["esg_rating"]      = data.get("peerEsgScorePerformance")
        _save(sus.T, EXTRAS_DIR / "esg" / f"{ticker}.parquet")
    except Exception:
        pass
    return result


def _fetch_options_summary(t: yf.Ticker, ticker: str) -> dict:
    """
    Capture implied volatility and put/call ratio from the nearest expiry.
    Returns a flat dict for the metrics snapshot.
    """
    result = {"ticker": ticker, "iv_call": None, "iv_put": None, "put_call_ratio": None}
    try:
        exps = t.options
        if not exps:
            return result
        chain = t.option_chain(exps[0])  # nearest expiry
        calls = chain.calls
        puts  = chain.puts

        # ATM implied vol: pick strike closest to last price
        last_price = t.fast_info.last_price
        if last_price and not calls.empty:
            atm_call = calls.iloc[(calls["strike"] - last_price).abs().argmin()]
            result["iv_call"] = atm_call.get("impliedVolatility")
        if last_price and not puts.empty:
            atm_put = puts.iloc[(puts["strike"] - last_price).abs().argmin()]
            result["iv_put"] = atm_put.get("impliedVolatility")

        # Put/call ratio by open interest
        total_call_oi = calls["openInterest"].sum()
        total_put_oi  = puts["openInterest"].sum()
        if total_call_oi > 0:
            result["put_call_ratio"] = total_put_oi / total_call_oi
    except Exception:
        pass
    return result


def _fetch_short_interest(info: dict, ticker: str) -> dict:
    """Extract short interest metrics from yfinance .info."""
    return {
        "ticker":             ticker,
        "shares_short":       info.get("sharesShort"),
        "shares_short_prior": info.get("sharesShortPriorMonth"),
        "short_ratio":        info.get("shortRatio"),
        "short_pct_float":    info.get("shortPercentOfFloat"),
        "date_short_interest":info.get("dateShortInterest"),
    }


def fetch_extras(tickers: list[str], skip_existing: bool = True):
    """
    Download all supplementary data for a list of tickers.
    Saves a combined metrics_extra.parquet with short interest + options summary.
    """
    EXTRAS_DIR.mkdir(parents=True, exist_ok=True)

    if skip_existing:
        tickers = [
            t for t in tickers
            if not (EXTRAS_DIR / "earnings_dates" / f"{t}.parquet").exists()
        ]
        print(f"[extras] {len(tickers)} tickers to process (skipping existing)")

    if not tickers:
        print("[extras] All tickers already done.")
        return

    all_metrics = []

    for idx, ticker in enumerate(tickers):
        print(f"  [{idx+1}/{len(tickers)}] {ticker}")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                t = _get_with_retry(ticker)
                if t is None:
                    break

                _fetch_analyst_estimates(t, ticker)
                _fetch_holders(t, ticker)
                _fetch_insider(t, ticker)
                _fetch_earnings_dates(t, ticker)
                options_row = _fetch_options_summary(t, ticker)
                rec_row     = _fetch_recommendations(t, ticker)
                esg_row     = _fetch_esg(t, ticker)

                # Short interest from .info
                try:
                    info = t.info
                    short_row = _fetch_short_interest(info, ticker)
                except Exception:
                    short_row = {"ticker": ticker}

                row = {
                    **short_row,
                    **{k: v for k, v in options_row.items() if k != "ticker"},
                    **{k: v for k, v in rec_row.items()     if k != "ticker"},
                    **{k: v for k, v in esg_row.items()     if k != "ticker"},
                }
                all_metrics.append(row)
                break

            except Exception as e:
                wait = 2 ** attempt + random.uniform(0, 1)
                print(f"  [retry {attempt}/{MAX_RETRIES}] {ticker}: {e} — waiting {wait:.1f}s")
                time.sleep(wait)

        if (idx + 1) % BATCH_SIZE == 0:
            sleep_secs = random.uniform(SLEEP_MIN, SLEEP_MAX)
            print(f"  [extras] Sleeping {sleep_secs:.1f}s...")
            time.sleep(sleep_secs)

    if all_metrics:
        metrics_df = pd.DataFrame(all_metrics).set_index("ticker")
        _save(metrics_df, EXTRAS_DIR / "metrics_extra.parquet")
        print(f"[extras] Saved metrics_extra: {len(metrics_df)} tickers")

    print("[extras] Done.")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.universe import get_universe
    tickers = get_universe()["ticker"].tolist()[:10]
    fetch_extras(tickers, skip_existing=False)
