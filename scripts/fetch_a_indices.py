import os, time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone

OUT_DIR = os.environ.get("OUT_DIR", "docs")
N_YEARS = int(os.environ.get("N_YEARS", "15"))
START = (datetime.now(timezone.utc) - timedelta(days=365*N_YEARS)).strftime("%Y-%m-%d")

INDEX_MAP = [
    ("上证指数", ["000001.SS", "^SSEC"]),
    ("沪深300", ["000300.SS"]),
    ("深证成指", ["399001.SZ"]),
    ("创业板指", ["399006.SZ"]),
]

def fetch_one(name, tickers):
    last_err = None
    for t in tickers:
        try:
            df = yf.download(t, start=START, interval="1d", auto_adjust=False, progress=False)
            if df is None or df.empty:
                last_err = f"No data for {t}"; continue
            df = df.rename(columns=str.lower).reset_index()
            df["ticker"] = t; df["label"] = name
            return df
        except Exception as e:
            last_err = str(e); time.sleep(1); continue
    raise RuntimeError(f"Failed to fetch {name}: {last_err}")

def main():
    os.makedirs(f"{OUT_DIR}/indices", exist_ok=True)
    frames = []
    for name, tlist in INDEX_MAP:
        df = fetch_one(name, tlist)
        df.to_csv(f"{OUT_DIR}/indices/{name}.csv", index=False, encoding="utf-8-sig")
        frames.append(df[["date","close","label"]])
    merged = None
    for df in frames:
        key = df["label"].iloc[0]
        ds = df[["date","close"]].rename(columns={"close": key})
        merged = ds if merged is None else pd.merge(merged, ds, on="date", how="outer")
    merged = merged.sort_values("date").reset_index(drop=True)
    merged.to_csv(f"{OUT_DIR}/indices_merged.csv", index=False, encoding="utf-8-sig")
    print("Saved indices to", OUT_DIR)

if __name__ == "__main__":
    main()
