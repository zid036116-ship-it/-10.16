import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone

OUT_DIR = os.environ.get("OUT_DIR", "docs")
N_YEARS = int(os.environ.get("N_YEARS", "10"))
START = (datetime.now(timezone.utc) - timedelta(days=365*N_YEARS)).strftime("%Y-%m-%d")
HOLDINGS_CSV = os.environ.get("HOLDINGS_CSV", "data/holdings.csv")

def load_holdings(path):
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "symbol" not in df.columns:
        raise ValueError("holdings.csv must contain a 'symbol' column")
    return [str(x).strip() for x in df["symbol"].dropna().unique() if str(x).strip()]

def fetch_price(sym):
    df = yf.download(sym, start=START, interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        return None
    out = df.rename(columns=str.lower).reset_index()
    out["symbol"] = sym
    return out

def try_fetch_flow(sym):
    try:
        import akshare as ak
        data = ak.stock_individual_fund_flow(stock=sym)
        if data is None or data.empty:
            return None
        data["symbol"] = sym
        return data
    except Exception:
        return None

def main():
    os.makedirs(f"{OUT_DIR}/holdings", exist_ok=True)
    os.makedirs(f"{OUT_DIR}/flows", exist_ok=True)
    syms = load_holdings(HOLDINGS_CSV)
    merged = None
    for s in syms:
        px = fetch_price(s)
        if px is not None and not px.empty:
            px.to_csv(f"{OUT_DIR}/holdings/{s}.csv", index=False, encoding="utf-8-sig")
            ds = px[["Date","Close"]].rename(columns={"Close": s})
            merged = ds if merged is None else pd.merge(merged, ds, on="Date", how="outer")
        fl = try_fetch_flow(s)
        if fl is not None and not fl.empty:
            fl.to_csv(f"{OUT_DIR}/flows/{s}_flow.csv", index=False, encoding="utf-8-sig")
    if merged is not None:
        merged.sort_values("Date").to_csv(f"{OUT_DIR}/ALL_TICKERS_MASTER.csv", index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
