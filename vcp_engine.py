import os
import pandas as pd
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
# GET LATEST AVAILABLE DATE
# =========================
def get_latest_date():
    res = supabase.table("stock_52w") \
        .select("date") \
        .order("date", desc=True) \
        .limit(1) \
        .execute()

    if not res.data:
        return None

    return res.data[0]["date"]


# =========================
# FETCH UNIVERSE
# =========================
def fetch_universe():
    latest_date = get_latest_date()

    if not latest_date:
        print("No data available in stock_52w")
        return pd.DataFrame(), None

    print("Using date:", latest_date)

    query = supabase.table("stock_52w") \
        .select("*") \
        .eq("date", latest_date) \
        .execute()

    df = pd.DataFrame(query.data)

    print("Raw rows fetched:", len(df))

    if df.empty:
        return df, latest_date

    # ===== FILTER (balanced) =====
    df = df[
        (df["close"] > df["sma50"]) &
        (df["sma50"] > df["sma200"]) &
        (df["pct_from_high"] >= -15) &
        (df["volume_ma20"] > 50000)
    ]

    print("Filtered universe:", len(df))

    return df, latest_date


# =========================
# FETCH PRICE DATA
# =========================
def fetch_price_data(ticker, latest_date):
    start_date = (
        datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)
    ).date().isoformat()

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
# SWING DETECTION
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
# CONTRACTIONS
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
# VALIDATE VCP
# =========================
def is_valid_vcp(drops):
    if len(drops) < 3:
        return False

    return all(drops[i] > drops[i + 1] for i in range(len(drops) - 1))


# =========================
# TIGHT RANGE
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
# SCORING
# =========================
def calculate_score(row, drops, valid_vcp, tight_range, volume_ratio):
    score = 0

    if row["close"] > row["sma50"] > row["sma200"]:
        score += 20

    if row["pct_from_high"] >= -5:
        score += 20
    elif row["pct_from_high"] >= -10:
        score += 15

    if valid_vcp:
        score += 25

    if volume_ratio < 0.6:
        score += 15

    if tight_range:
        score += 10

    if row["pct_from_low"] > 50:
        score += 10

    return score


# =========================
# PROCESS STOCK
# =========================
def process_stock(row, latest_date):
    ticker = row["ticker"]

    df = fetch_price_data(ticker, latest_date)
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
        "date": latest_date,
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
# UPSERT
# =========================
def upsert_results(results):
    if not results:
        print("No results to store.")
        return

    supabase.table("vcp_candidates").upsert(results).execute()
    print(f"Stored {len(results)} results.")


# =========================
# MAIN
# =========================
def run_vcp_engine():
    print("Fetching universe...")

    universe, latest_date = fetch_universe()

    if universe.empty:
        print("No stocks found after filtering.")
        return

    results = []

    print(f"Processing {len(universe)} stocks...")

    for _, row in universe.iterrows():
        try:
            res = process_stock(row, latest_date)
            if res:
                results.append(res)
        except Exception as e:
            print(f"Error processing {row['ticker']}: {e}")

    upsert_results(results)

    print("VCP Engine completed.")


# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    try:
        run_vcp_engine()
    except Exception as e:
        print("Fatal error:", e)
