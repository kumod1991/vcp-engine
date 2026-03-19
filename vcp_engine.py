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

LOOKBACK_DAYS = 150


# =========================
# CLEAN VALUE (JSON SAFE)
# =========================
def clean_value(val):
    if val is None:
        return None
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
    return val


def sanitize(record):
    return {k: clean_value(v) for k, v in record.items()}


# =========================
# GET LATEST DATE
# =========================
def get_latest_date():
    res = supabase.table("stock_prices_daily") \
        .select("date") \
        .order("date", desc=True) \
        .limit(1) \
        .execute()

    return res.data[0]["date"]


# =========================
# FETCH ALL STOCKS (PAGINATION)
# =========================
def fetch_universe():
    all_rows = []
    offset = 0
    limit = 1000

    while True:
        res = supabase.table("stock_52w") \
            .select("*") \
            .range(offset, offset + limit - 1) \
            .execute()

        data = res.data

        if not data:
            break

        all_rows.extend(data)
        offset += limit

    df = pd.DataFrame(all_rows)
    print("Universe size:", len(df))

    return df


# =========================
# FETCH PRICE DATA
# =========================
def fetch_price_data(ticker, latest_date):
    start_date = (
        datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)
    ).date().isoformat()

    res = supabase.table("stock_prices_daily") \
        .select("*") \
        .eq("ticker", ticker) \
        .gte("date", start_date) \
        .order("date") \
        .execute()

    df = pd.DataFrame(res.data)

    if df.empty or len(df) < 60:
        return None

    return df


# =========================
# SWING DETECTION
# =========================
def find_swings(df, window=5):
    highs, lows = [], []

    for i in range(window, len(df) - window):
        high = df["high"].iloc[i]
        low = df["low"].iloc[i]

        if high == df["high"].iloc[i-window:i+window].max():
            highs.append((i, high))

        if low == df["low"].iloc[i-window:i+window].min():
            lows.append((i, low))

    return highs, lows


# =========================
# CONTRACTIONS
# =========================
def calculate_contractions(highs, lows):
    drops = []

    for i in range(min(len(highs), len(lows)) - 1):
        high = highs[i][1]
        low = lows[i][1]

        if high and high > 0:
            drop = ((high - low) / high) * 100
            drops.append(round(drop, 2))

    return drops


# =========================
# VALIDATE VCP
# =========================
def is_valid_vcp(drops):
    return len(drops) >= 3 and all(drops[i] > drops[i+1] for i in range(len(drops)-1))


# =========================
# TIGHT RANGE
# =========================
def check_tight_range(df):
    recent = df.tail(10)
    high = recent["high"].max()
    low = recent["low"].min()

    if not high:
        return False

    return ((high - low) / high) * 100 < 8


# =========================
# SCORE
# =========================
def calculate_score(row, drops, tight, vol_ratio):
    score = 0

    if row["close"] > row["sma50"] > row["sma200"]:
        score += 20

    if row["pct_from_high"] >= -5:
        score += 20
    elif row["pct_from_high"] >= -10:
        score += 15

    score += 25  # valid VCP

    if vol_ratio < 0.6:
        score += 15

    if tight:
        score += 10

    if row["pct_from_low"] > 50:
        score += 10

    return score


# =========================
# PROCESS STOCK
# =========================
def process_stock(row, latest_date):
    # basic sanity
    if not (row["close"] and row["sma50"] and row["close"] > row["sma50"]):
        return None

    df = fetch_price_data(row["ticker"], latest_date)
    if df is None:
        return None

    highs, lows = find_swings(df)
    drops = calculate_contractions(highs, lows)

    if not is_valid_vcp(drops):
        return None

    tight = check_tight_range(df)

    vol_ratio = (
        row["volume"] / row["volume_ma20"]
        if row["volume_ma20"] and row["volume_ma20"] > 0
        else 1
    )

    score = calculate_score(row, drops, tight, vol_ratio)

    if score < 50:
        return None

    category = "IDEAL" if score >= 80 else "DEVELOPING"

    return sanitize({
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "vcp_score": int(score),
        "category": category,
        "contractions": len(drops),
        "contraction_pattern": " → ".join([f"{d}%" for d in drops[:4]]),
        "pct_from_high": row["pct_from_high"],
        "base_depth": row["pct_from_low"],
        "volume_ratio": round(vol_ratio, 2),
        "volume_dryup": vol_ratio < 0.6,
        "tight_range": tight,
        "near_pivot": row["pct_from_high"] >= -10,
        "breakout_level": df["high"].tail(20).max(),
    })


# =========================
# UPSERT
# =========================
def upsert_results(results):
    if not results:
        print("No VCP candidates found.")
        return

    supabase.table("vcp_candidates").upsert(results).execute()
    print(f"Stored {len(results)} results.")


# =========================
# MAIN
# =========================
def run():
    print("Fetching universe...")
    universe = fetch_universe()

    if universe.empty:
        print("No stocks found.")
        return

    latest_date = get_latest_date()
    print("Using price date:", latest_date)

    results = []

    for _, row in universe.iterrows():
        try:
            res = process_stock(row, latest_date)
            if res:
                results.append(res)
        except Exception as e:
            print("Error:", row["ticker"], e)

    upsert_results(results)
    print("VCP Engine completed.")


# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    run()
