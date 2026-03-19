import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client

# =========================
# CONFIG
# =========================
SUPABASE_URL = os.environ("SUPABASE_URL")
SUPABASE_KEY = os.environ("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LOOKBACK_DAYS = 150


# =========================
# STEP 1: FETCH UNIVERSE
# =========================
def fetch_universe():
    today = datetime.today().date().isoformat()

    query = supabase.table("stock_52w") \
        .select("*") \
        .eq("date", today) \
        .gt("close", 0) \
        .execute()

    df = pd.DataFrame(query.data)

    if df.empty:
        return df

    # Apply filters in Python (more flexible than SQL)
    df = df[
        (df["close"] > df["sma50"]) &
        (df["sma50"] > df["sma200"]) &
        (df["pct_from_high"] >= -12) &
        (df["pct_from_low"] >= 25) &
        (df["volume_ma20"] > 100000)
    ]

    return df


# =========================
# STEP 2: FETCH PRICE DATA
# =========================
def fetch_price_data(ticker):
    start_date = (datetime.today() - timedelta(days=LOOKBACK_DAYS)).date().isoformat()

    query = supabase.table("stock_prices_daily") \
        .select("*") \
        .eq("ticker", ticker) \
        .gte("date", start_date) \
        .order("date") \
        .execute()

    df = pd.DataFrame(query.data)

    if df.empty or len(df) < 60:
        return None

    return df


# =========================
# STEP 3: SWING DETECTION
# =========================
def find_swings(df, window=5):
    highs, lows = [], []

    for i in range(window, len(df) - window):
        high = df["high"].iloc[i]
        low = df["low"].iloc[i]

        if high == df["high"].iloc[i - window:i + window].max():
            highs.append((i, high))

        if low == df["low"].iloc[i - window:i + window].min():
            lows.append((i, low))

    return highs, lows


# =========================
# STEP 4: BUILD CONTRACTIONS
# =========================
def calculate_contractions(highs, lows):
    drops = []

    for i in range(min(len(highs), len(lows)) - 1):
        high = highs[i][1]
        low = lows[i][1]

        if high > 0:
            drop_pct = ((high - low) / high) * 100
            drops.append(round(drop_pct, 2))

    return drops


# =========================
# STEP 5: VALIDATE VCP
# =========================
def is_valid_vcp(drops):
    if len(drops) < 3:
        return False

    for i in range(len(drops) - 1):
        if drops[i] <= drops[i + 1]:
            return False

    return True


# =========================
# STEP 6: TIGHT RANGE
# =========================
def check_tight_range(df):
    recent = df.tail(10)

    high = recent["high"].max()
    low = recent["low"].min()

    if high == 0:
        return False

    range_pct = ((high - low) / high) * 100

    return range_pct < 8


# =========================
# STEP 7: SCORING
# =========================
def calculate_score(row, drops, valid_vcp, tight_range, volume_ratio):
    score = 0

    # Trend
    if row["close"] > row["sma50"] > row["sma200"]:
        score += 20

    # Position
    if row["pct_from_high"] >= -5:
        score += 20
    elif row["pct_from_high"] >= -10:
        score += 15

    # Contraction
    if valid_vcp:
        score += 25

    # Volume
    if volume_ratio < 0.6:
        score += 15

    # Tightness
    if tight_range:
        score += 10

    # Prior move
    if row["pct_from_low"] > 50:
        score += 10

    return score


# =========================
# STEP 8: PROCESS ONE STOCK
# =========================
def process_stock(row):
    ticker = row["ticker"]

    df = fetch_price_data(ticker)
    if df is None:
        return None

    highs, lows = find_swings(df)
    drops = calculate_contractions(highs, lows)

    valid_vcp = is_valid_vcp(drops)
    tight_range = check_tight_range(df)

    volume_ratio = row["volume"] / row["volume_ma20"] if row["volume_ma20"] else 1

    near_pivot = row["pct_from_high"] >= -10

    breakout_level = df["high"].tail(20).max()

    score = calculate_score(row, drops, valid_vcp, tight_range, volume_ratio)

    return {
        "date": datetime.today().date().isoformat(),
        "ticker": ticker,
        "exchange": row["exchange"],

        "vcp_score": int(score),
        "contractions": len(drops),
        "contraction_pattern": " → ".join([f"{d}%" for d in drops[:4]]),

        "pct_from_high": row["pct_from_high"],
        "base_depth": row["pct_from_low"],

        "volume_ratio": round(volume_ratio, 2),
        "volume_dryup": volume_ratio < 0.6,

        "tight_range": tight_range,
        "near_pivot": near_pivot,

        "breakout_level": breakout_level
    }


# =========================
# STEP 9: UPSERT RESULTS
# =========================
def upsert_results(results):
    if not results:
        return

    supabase.table("vcp_candidates").upsert(results).execute()


# =========================
# MAIN RUNNER
# =========================
def run_vcp_engine():
    print("Fetching universe...")
    universe = fetch_universe()

    if universe.empty:
        print("No stocks found.")
        return

    results = []

    print(f"Processing {len(universe)} stocks...")

    for _, row in universe.iterrows():
        try:
            res = process_stock(row)
            if res:
                results.append(res)
        except Exception as e:
            print(f"Error processing {row['ticker']}: {e}")

    print(f"Storing {len(results)} results...")
    upsert_results(results)

    print("VCP Engine completed.")


# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    run_vcp_engine()