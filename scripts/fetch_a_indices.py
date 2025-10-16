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
    for tkr in tickers:
        try:
            df = yf.download(tkr, start=START, interval="1d", auto_adjust=False, progress=False)
            if df is None or df.empty:
                last_err = f"No data for {tkr}"
                continue
            # ✅ 先把索引复位，再统一小写列名
            df = df.reset_index().rename(columns=lambda x: str(x).lower())
            # 保底：有些环境返回 'adj close' 等
            need = {"date","close"}
            if not need.issubset(set(df.columns)):
                raise RuntimeError(f"{tkr} columns={list(df.columns)} missing {need - set(df.columns)}")
            df["ticker"] = tkr
            df["label"]  = name
            return df
        except Exception as e:
            last_err = str(e)
            time.sleep(1)
            continue
    raise RuntimeError(f"Failed to fetch {name}: {last_err}")

def main():
    os.makedirs(f"{OUT_DIR}/indices", exist_ok=True)
    frames = []
    for name, tlist in INDEX_MAP:
        df = fetch_one(name, tlist)
        df.to_csv(f"{OUT_DIR}/indices/{name}.csv", index=False, encoding="utf-8-sig")
        frames.append(df[["date","close","label"]])

    # 合并成 wide
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
