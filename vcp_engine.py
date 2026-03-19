import os
import pandas as pd
import math
from datetime import datetime, timedelta
from supabase import create_client

# =========================
# CONFIG
# =========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LOOKBACK_DAYS = 120   # reduced
PROCESS_WINDOW = 80   # core optimization


# =========================
# CLEAN VALUES
# =========================
def clean(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def sanitize(d):
    return {k: clean(v) for k, v in d.items()}


# =========================
# FETCH UNIVERSE
# =========================
def fetch_universe():
    all_rows, offset, limit = [], 0, 1000

    while True:
        res = supabase.table("stock_52w") \
            .select("*") \
            .range(offset, offset + limit - 1) \
            .execute()

        if not res.data:
            break

        all_rows.extend(res.data)
        offset += limit

    df = pd.DataFrame(all_rows)
    print("Universe size:", len(df))
    return df


# =========================
# GET LATEST DATE
# =========================
def get_latest_date():
    return supabase.table("stock_prices_daily") \
        .select("date") \
        .order("date", desc=True) \
        .limit(1) \
        .execute().data[0]["date"]


# =========================
# FETCH FILTERED PRICE DATA
# =========================
def fetch_price_data(tickers, latest_date):
    start_date = (
        datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)
    ).date().isoformat()

    all_rows = []
    batch_size = 200

    print("Fetching price data...")

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]

        res = supabase.table("stock_prices_daily") \
            .select("ticker,date,high,low,close,volume") \
            .in_("ticker", batch) \
            .gte("date", start_date) \
            .execute()

        if res.data:
            all_rows.extend(res.data)

    df = pd.DataFrame(all_rows)

    print("Price rows:", len(df))
    return df


# =========================
# SWING DETECTION
# =========================
def find_swings(df, window=5):
    highs, lows = [], []

    for i in range(window, len(df) - window):
        h = df["high"].iloc[i]
        l = df["low"].iloc[i]

        if h == df["high"].iloc[i-window:i+window].max():
            highs.append((i, h))

        if l == df["low"].iloc[i-window:i+window].min():
            lows.append((i, l))

    return highs, lows


# =========================
# CONTRACTIONS
# =========================
def contractions(highs, lows):
    drops = []

    for i in range(min(len(highs), len(lows)) - 1):
        h = highs[i][1]
        l = lows[i][1]

        if h > 0:
            drops.append(round((h - l) / h * 100, 2))

    return drops


def valid_vcp(drops):
    return len(drops) >= 3 and all(drops[i] > drops[i+1] for i in range(len(drops)-1))


def tight_range(df):
    r = df.tail(10)
    h, l = r["high"].max(), r["low"].min()
    return h > 0 and ((h - l) / h * 100 < 8)


# =========================
# SCORE
# =========================
def score(row, drops, tight, vol_ratio):
    s = 0

    if row["close"] > row["sma50"] > row["sma200"]:
        s += 20

    if row["pct_from_high"] >= -5:
        s += 20
    elif row["pct_from_high"] >= -10:
        s += 15

    s += 25  # valid VCP

    if vol_ratio < 0.6:
        s += 15

    if tight:
        s += 10

    if row["pct_from_low"] > 50:
        s += 10

    return s


# =========================
# MAIN ENGINE
# =========================
def run():
    print("Fetching universe...")
    universe = fetch_universe()

    if universe.empty:
        print("No stocks found")
        return

    latest_date = get_latest_date()
    print("Using date:", latest_date)

    tickers = universe["ticker"].dropna().unique().tolist()

    price_df = fetch_price_data(tickers, latest_date)

    if price_df.empty:
        print("No price data")
        return

    # SORT ONCE (IMPORTANT)
    price_df = price_df.sort_values(["ticker", "date"])

    grouped = dict(tuple(price_df.groupby("ticker")))

    results = []

    print("Processing stocks...")

    for _, row in universe.iterrows():
        ticker = row["ticker"]

        if ticker not in grouped:
            continue

        # 🔥 EARLY FILTERS (BIG SPEED WIN)
        if row["pct_from_high"] is None or row["pct_from_high"] < -15:
            continue

        if not (row["close"] and row["sma50"] and row["close"] > row["sma50"]):
            continue

        df = grouped[ticker].tail(PROCESS_WINDOW)

        if len(df) < 60:
            continue

        highs, lows = find_swings(df)
        drops = contractions(highs, lows)

        if not valid_vcp(drops):
            continue

        tight = tight_range(df)

        vol_ratio = (
            row["volume"] / row["volume_ma20"]
            if row["volume_ma20"] and row["volume_ma20"] > 0
            else 1
        )

        s = score(row, drops, tight, vol_ratio)

        if s < 50:
            continue

        results.append(sanitize({
            "ticker": ticker,
            "exchange": row["exchange"],
            "vcp_score": int(s),
            "category": "IDEAL" if s >= 80 else "DEVELOPING",
            "contractions": len(drops),
            "contraction_pattern": " → ".join([f"{d}%" for d in drops[:4]]),
            "pct_from_high": row["pct_from_high"],
            "base_depth": row["pct_from_low"],
            "volume_ratio": round(vol_ratio, 2),
            "volume_dryup": vol_ratio < 0.6,
            "tight_range": tight,
            "near_pivot": row["pct_from_high"] >= -10,
            "breakout_level": df["high"].tail(20).max(),
        }))

    if results:
        supabase.table("vcp_candidates").upsert(results).execute()

    print(f"Stored {len(results)} results")
    print("VCP Engine completed.")


# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    run()
