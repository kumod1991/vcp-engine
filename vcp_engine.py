import os
import pandas as pd
import math
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client

# =========================
# CONFIG
# =========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LOOKBACK_DAYS = 150
PROCESS_WINDOW = 100


# =========================
# SANITIZE
# =========================
def clean(v):
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
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
# FETCH PRICE DATA
# =========================
def fetch_price_data(tickers, latest_date):
    start_date = (
        datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)
    ).date().isoformat()

    all_rows = []
    batch_size = 200

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
# CORE LOGIC
# =========================

def prior_uptrend(df):
    if len(df) < 100:
        return False
    past = df.iloc[-100:-50]
    recent = df.iloc[-50:]
    return recent["close"].mean() > past["close"].mean() * 1.2


def base_near_high(row):
    return row["pct_from_high"] >= -15


def ma_alignment(row):
    return (
        row["close"] > row["sma50"] > row["sma150"] > row["sma200"]
    )


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


def contractions(highs, lows):
    drops = []
    for i in range(min(len(highs), len(lows)) - 1):
        h = highs[i][1]
        l = lows[i][1]
        if h > 0:
            drops.append(round((h - l) / h * 100, 2))
    return drops


def valid_vcp(drops):
    if len(drops) < 3:
        return False

    improving = all(drops[i] > drops[i+1] for i in range(len(drops)-1))

    if not improving:
        violations = sum(drops[i] <= drops[i+1] for i in range(len(drops)-1))
        if violations > 1:
            return False

    if drops[0] < 8:
        return False

    if drops[-1] > 12:
        return False

    return True


def volume_dryup(df):
    recent = df.tail(10)
    avg_recent = recent["volume"].mean()
    avg_prior = df.tail(40).head(30)["volume"].mean()

    return avg_recent < avg_prior * 0.7


def tight_pivot(df):
    recent = df.tail(5)
    high = recent["high"].max()
    low = recent["low"].min()
    return (high - low) / high * 100 < 5


def score(row, drops):
    s = 50
    s += max(0, 20 - drops[-1])

    if row["pct_from_high"] >= -5:
        s += 10

    if row["pct_from_low"] > 60:
        s += 10

    return min(s, 100)


# =========================
# MAIN ENGINE
# =========================
def run():
    print("Fetching universe...")
    universe = fetch_universe()

    if universe.empty:
        return

    latest_date = get_latest_date()
    print("Using date:", latest_date)

    tickers = universe["ticker"].dropna().unique().tolist()
    price_df = fetch_price_data(tickers, latest_date)

    price_df = price_df.sort_values(["ticker", "date"])
    grouped = dict(tuple(price_df.groupby("ticker")))

    results = []

    print("Processing stocks...")

    for _, row in universe.iterrows():
        ticker = row["ticker"]

        if ticker not in grouped:
            continue

        df_full = grouped[ticker]

        if len(df_full) < 120:
            continue

        df = df_full.tail(PROCESS_WINDOW)

        # --- Filters ---
        if not prior_uptrend(df_full):
            continue

        if not base_near_high(row):
            continue

        if not ma_alignment(row):
            continue

        highs, lows = find_swings(df)
        drops = contractions(highs, lows)

        if not valid_vcp(drops):
            continue

        if not volume_dryup(df):
            continue

        if not tight_pivot(df):
            continue

        s = score(row, drops)

        results.append(sanitize({
            "ticker": ticker,
            "exchange": row["exchange"],
            "vcp_score": int(s),
            "category": "IDEAL",
            "contractions": int(len(drops)),
            "contraction_pattern": " → ".join([f"{d}%" for d in drops[:4]]),
            "pct_from_high": float(row["pct_from_high"]),
            "base_depth": float(row["pct_from_low"]),
            "volume_dryup": True,
            "tight_range": True,
            "near_pivot": True,
            "breakout_level": float(df["high"].tail(20).max()),
            "detected_at": datetime.utcnow().isoformat()
        }))

    # =========================
    # SNAPSHOT OVERWRITE
    # =========================
    print("Clearing previous VCP data...")
    supabase.table("vcp_candidates").delete().neq("ticker", "").execute()

    if results:
        supabase.table("vcp_candidates").insert(results).execute()

    print(f"Stored {len(results)} results")
    print("VCP Engine completed.")


# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    run()
