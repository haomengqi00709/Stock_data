# Autotrader2 — Quant Data Pipeline

Institutional-grade data pipeline for NASDAQ + NYSE stocks ($500M+ market cap).

## Setup

```bash
pip install -r requirements.txt
```

Copy the env template and fill in your API keys:
```bash
cp .env.example .env
```

## API Keys

| Key | Required | Get it at |
|---|---|---|
| `FRED_API_KEY` | Yes (for macro data) | https://fred.stlouisfed.org/docs/api/api_key.html |
| `GEMINI_API_KEY` | Optional (AI enrichment) | https://aistudio.google.com |

## Run

```bash
# Full overnight pipeline — prices, fundamentals, extras, macro for ~2,800 tickers
python3 overnight.py

# $1B+ market cap filter (~1,800 tickers, faster)
python3 overnight.py --min-mcap 1000000000

# Skip smoke test and go straight to full run
python3 overnight.py --skip-test
```

## Check data after run

```bash
python3 check_prices.py          # coverage report for all price files
python3 check_combined.py        # inspect combined price parquet
python3 check_combined.py --ticker AAPL --tail
```

## Data collected

**Per ticker (~2,800 tickers):**
- OHLCV prices — adjusted, 10 years
- Dividends & splits
- Income statement, balance sheet, cash flow (annual + quarterly)
- Key metrics: PE, PB, ROE, margins, etc.
- Analyst estimates, upgrades/downgrades, recommendations
- Institutional & mutual fund holders
- Insider transactions
- Earnings announcement dates
- Options: ATM implied volatility, put/call ratio
- Short interest

**Market-wide (macro):**
- Fed funds rate, CPI, unemployment, GDP
- Treasury yields (2Y, 10Y), yield curve spread
- High yield & investment grade credit spreads
- VIX, S&P 500, Nasdaq, Russell 2000, DXY, Gold, Oil

## Project structure

```
autotrader2/
├── overnight.py          # Main pipeline orchestrator
├── main.py               # Alternative CLI runner
├── check_prices.py       # Price data coverage checker
├── check_combined.py     # Combined parquet inspector
├── config.py             # Paths and settings
├── requirements.txt
├── src/
│   ├── universe.py       # Ticker universe (NASDAQ+NYSE screener)
│   ├── prices.py         # OHLCV, dividends, splits
│   ├── fundamentals.py   # Financial statements + metrics
│   ├── extras.py         # Analyst, holders, insider, options, ESG
│   ├── macro.py          # FRED + market indicators
│   └── storage.py        # DuckDB query layer
└── data/                 # Generated — not in git
    ├── raw/
    ├── processed/
    └── db/
```
