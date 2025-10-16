# A-share Market Data Pipeline (Indices + Holdings + Optional Money Flows)

This repo fetches daily time series for key A-share indices (上证/沪深300/深成指/创业板), your holdings' OHLCV,
and optional per-stock capital flows via `akshare` when reachable. Outputs are saved to `docs/` for GitHub Pages.

## Quick start
1. Edit `data/holdings.csv` with your tickers (Yahoo format, e.g., 600519.SS, 000001.SZ).
2. Push the repo to GitHub.
3. Enable Actions (for the workflow) and Pages (root = `main` branch).
4. The workflow runs on weekdays 09:30 Asia/Shanghai and on-demand.

See comments in `scripts/*.py` for details.
