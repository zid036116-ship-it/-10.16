# scripts/fetch_holdings_and_flows.py
# -----------------------------------------------------------------------------
# Fetch daily OHLCV for your holdings (from data/holdings.csv) via yfinance,
# optionally fetch per-stock capital flows via akshare (if reachable),
# and export merged wide close to docs/ALL_TICKERS_MASTER.csv.
#
# Notes:
# - Assumes Yahoo-format tickers in data/holdings.csv (column name: symbol)
# - Robust to missing tickers or network hiccups
# - Key fix: in fetch_price(), we do reset_index() BEFORE lowercasing columns,
#   so that "Date" becomes a regular column first -> then rename to "date".
# -----------------------------------------------------------------------------

import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

OUT_DIR = os.environ.get("OUT_DIR", "docs")
N_YEARS = int(os.environ.get("N_YEARS", "10"))
START = (datetime.now(timezone.utc) - timedelta(days=365 * N_YEARS)).strftime("%Y-%m-%d")
HOLDINGS_CSV = os.environ.get("HOLDINGS_CSV", "data/holdings.csv")

# ----------------------------------------
# Utilities
# ----------------------------------------
def log(msg: str):
    print(f"[fetch_holdings] {msg}", flush=True)

def load_holdings(path: str) -> list:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Holdings file not found: {path}")
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "symbol" not in df.columns:
        raise ValueError("holdings.csv must contain a 'symbol' column")
    syms = [str(x).strip() for x in df["symbol"].dropna().unique() if str(x).strip()]
    log(f"Loaded {len(syms)} symbols from {path}: {syms}")
    return syms

def fetch_price(sym: str, max_retries: int = 2, sleep_sec: int = 1) -> pd.DataFrame | None:
    """
    Fetch daily OHLCV from yfinance.
    🔧 FIX: We do reset_index() BEFORE lowercasing columns, so 'Date' -> column first -> 'date'.
    """
    for attempt in range(1, max_retries + 1):
        try:
            df = yf.download(sym, start=START, interval="1d", auto_adjust=False, progress=False)
            if df is None or df.empty:
                log(f"[{sym}] no data returned by yfinance")
                return None
            # ✅ critical order: reset index first, then lowercase column names
            out = df.reset_index().rename(columns=lambda x: str(x).lower())
            # We expect 'date' and 'close' columns to exist now
            need = {"date", "close"}
            if not need.issubset(set(out.columns)):
                log(f"[{sym}] missing columns after rename; got {list(out.columns)}")
                return None
            out["symbol"] = sym
            return out
        except Exception as e:
            log(f"[{sym}] fetch attempt {attempt}/{max_retries} failed: {e}")
            time.sleep(sleep_sec)
    return None

def try_fetch_flow(sym: str) -> pd.DataFrame | None:
    """
    Optional: fetch per-stock capital flows via akshare.
    If akshare/network/vendor not available, return None gracefully.
    """
    try:
        import akshare as ak
        # Different akshare versions may have different endpoints; keep this simple.
        data = ak.stock_individual_fund_flow(stock=sym)
        if data is None or data.empty:
            return None
        data = data.copy()
        data["symbol"] = sym
        return data
    except Exception as e:
        log(f"[{sym}] flow fetch skipped (akshare/network): {e}")
        return None

# ----------------------------------------
# Main
# ----------------------------------------
def main():
    os.makedirs(f"{OUT_DIR}/holdings", exist_ok=True)
    os.makedirs(f"{OUT_DIR}/flows", exist_ok=True)

    syms = load_holdings(HOLDINGS_CSV)

    # Merge wide close
    merged = None
    saved_count = 0
    for s in syms:
        px = fetch_price(s)
        if px is not None and not px.empty:
            # Save per-symbol OHLCV
            px.to_csv(f"{OUT_DIR}/holdings/{s}.csv", index=False, encoding="utf-8-sig")
            saved_count += 1

            # Contribute to wide merge (date + close)
            ds = px[["date", "close"]].rename(columns={"close": s})
            merged = ds if merged is None else pd.merge(merged, ds, on="date", how="outer")

        # Flow is optional; never fail the whole job
        fl = try_fetch_flow(s)
        if fl is not None and not fl.empty:
            # Keep original columns to help later inspection; save as-is
            fl.to_csv(f"{OUT_DIR}/flows/{s}_flow.csv", index=False, encoding="utf-8-sig")

    if merged is not None:
        merged = merged.sort_values("date").reset_index(drop=True)
        merged.to_csv(f"{OUT_DIR}/ALL_TICKERS_MASTER.csv", index=False, encoding="utf-8-sig")
        log(f"Saved ALL_TICKERS_MASTER.csv with columns: {list(merged.columns)}")

    log(f"Saved {saved_count} holding OHLCV files to {OUT_DIR}/holdings/")
    log("Done.")

if __name__ == "__main__":
    main()
