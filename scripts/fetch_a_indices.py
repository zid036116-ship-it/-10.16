# scripts/fetch_a_indices.py
# -----------------------------------------------------------------------------
# Robust A-share index fetcher:
# 1) Try yfinance first (Yahoo tickers)
# 2) If Yahoo fails/empty -> fallback to akshare (东财/上交所数据接口)
# 3) If某个指数最终仍失败，记录 warning，继续处理其他指数
# 4) 输出:
#    - docs/indices/上证指数.csv 等个别文件
#    - docs/indices_merged.csv (wide, 按 date 合并)
# -----------------------------------------------------------------------------

import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

OUT_DIR = os.environ.get("OUT_DIR", "docs")
N_YEARS = int(os.environ.get("N_YEARS", "15"))
START = (datetime.now(timezone.utc) - timedelta(days=365 * N_YEARS)).strftime("%Y-%m-%d")

# 统一映射：首选 Yahoo 符号；akshare 作为兜底（注意 akshare 用的是国内代码格式）
INDEX_MAP = [
    # name, yahoo_symbol, akshare_symbol
    ("上证指数",  "000001.SS", "sh000001"),
    ("沪深300",  "000300.SS", "sh000300"),
    ("深证成指",  "399001.SZ", "sz399001"),
    ("创业板指",  "399006.SZ", "sz399006"),
]

def log(msg: str):
    print(f"[fetch_indices] {msg}", flush=True)

def fetch_yahoo(symbol: str) -> pd.DataFrame | None:
    """Try yfinance. Return columns: date, open, high, low, close, volume ..."""
    try:
        import yfinance as yf
        df = yf.download(symbol, start=START, interval="1d", auto_adjust=False, progress=False)
        if df is None or df.empty:
            log(f"[Yahoo] {symbol} -> empty")
            return None
        out = df.reset_index().rename(columns=lambda x: str(x).lower())
        if "date" not in out.columns or "close" not in out.columns:
            log(f"[Yahoo] {symbol} -> missing columns: {list(out.columns)}")
            return None
        out["source"] = "yahoo"
        return out
    except Exception as e:
        log(f"[Yahoo] {symbol} -> error: {e}")
        return None

def fetch_akshare(symbol: str) -> pd.DataFrame | None:
    """Fallback to akshare. Return columns: date, open, high, low, close, volume."""
    try:
        import akshare as ak
        # akshare 指数日线
        df = ak.stock_zh_index_daily(symbol=symbol)
        if df is None or df.empty:
            log(f"[akshare] {symbol} -> empty")
            return None
        # ak 的列一般是: date, open, high, low, close, volume, outstand, turnover 等
        out = df.copy()
        # 统一小写
        out.columns = [str(c).lower() for c in out.columns]
        # ak 有时 date 是 str
        out["date"] = pd.to_datetime(out["date"])
        # 只保留常用列
        keep = [c for c in ["date","open","high","low","close","volume"] if c in out.columns]
        out = out[keep]
        out["source"] = "akshare"
        # 过滤起始日
        out = out[out["date"] >= pd.to_datetime(START)].reset_index(drop=True)
        return out
    except Exception as e:
        log(f"[akshare] {symbol} -> error: {e}")
        return None

def fetch_one(name: str, yahoo_symbol: str, ak_symbol: str) -> pd.DataFrame | None:
    """Try Yahoo first; if fail, try akshare. Return unified df with date/close/label/ticker/source."""
    # 1) Yahoo
    ydf = fetch_yahoo(yahoo_symbol)
    if ydf is not None:
        ydf["label"] = name
        ydf["ticker"] = yahoo_symbol
        return ydf

    # 2) akshare
    adf = fetch_akshare(ak_symbol)
    if adf is not None:
        adf["label"] = name
        adf["ticker"] = ak_symbol
        return adf

    # 3) both failed
    log(f"[WARN] Both Yahoo({yahoo_symbol}) and akshare({ak_symbol}) failed for {name}")
    return None

def main():
    os.makedirs(f"{OUT_DIR}/indices", exist_ok=True)

    frames = []
    for name, ysym, asym in INDEX_MAP:
        df = fetch_one(name, ysym, asym)
        if df is None or df.empty:
            # 不中断流程，跳过
            log(f"[SKIP] {name} no data from any source.")
            continue

        # 保存个别文件
        out_path = f"{OUT_DIR}/indices/{name}.csv"
        df.sort_values("date").to_csv(out_path, index=False, encoding="utf-8-sig")
        frames.append(df[["date","close","label"]])
        log(f"[OK] Saved {name} -> {out_path}; rows={len(df)}; source={df['source'].iloc[0]}")

    if not frames:
        raise SystemExit("[FATAL] No index file saved; all sources failed. Check network or symbols.")

    # 合并成 wide
    merged = None
    for df in frames:
        key = df["label"].iloc[0]
        ds = df[["date","close"]].rename(columns={"close": key})
        merged = ds if merged is None else pd.merge(merged, ds, on="date", how="outer")

    merged = merged.sort_values("date").reset_index(drop=True)
    merged.to_csv(f"{OUT_DIR}/indices_merged.csv", index=False, encoding="utf-8-sig")
    log(f"[OK] Saved indices_merged.csv with columns: {list(merged.columns)}")

if __name__ == "__main__":
    main()
