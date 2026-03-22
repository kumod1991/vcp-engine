import os
import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from supabase import create_client

# ================= CONFIG =================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LOOKBACK_DAYS = 180
PROCESS_WINDOW = 120


# ================= HELPERS =================
def clean(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


def sanitize(d):
    return {k: clean(v) for k, v in d.items()}


# ================= FETCH =================
def fetch_universe():
    res = supabase.table("stock_52w").select("*").execute()
    df = pd.DataFrame(res.data)
    return df


def get_latest_date():
    return supabase.table("stock_prices_daily") \
        .select("date") \
        .order("date", desc=True) \
        .limit(1) \
        .execute().data[0]["date"]


def fetch_price_data(tickers, latest_date):
    start = (
        datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)
    ).date().isoformat()

    res = supabase.table("stock_prices_daily") \
        .select("ticker,date,high,low,close,volume") \
        .in_("ticker", tickers) \
        .gte("date", start) \
        .execute()

    df = pd.DataFrame(res.data)
    return df


def fetch_indicators():
    res = supabase.table("indicators") \
        .select("ticker,exchange,sma50,sma150,sma200,rs_rating") \
        .execute()

    return pd.DataFrame(res.data)


# ================= CORE LOGIC =================

def prior_uptrend(df):
    if len(df) < 100:
        return False
    return df["close"].iloc[-50:].mean() > df["close"].iloc[-100:-50].mean() * 1.2


def ma_alignment(row):
    return row["close"] > row["sma50"] > row["sma150"] > row["sma200"]


def find_swings(df, window=5, threshold=0.03):
    highs, lows = [], []

    for i in range(window, len(df) - window):
        h = df["high"].iloc[i]
        l = df["low"].iloc[i]

        local_high = df["high"].iloc[i-window:i+window].max()
        local_low = df["low"].iloc[i-window:i+window].min()

        if h == local_high:
            if not highs or abs(h - highs[-1][1]) / highs[-1][1] > threshold:
                highs.append((i, h))

        if l == local_low:
            if not lows or abs(l - lows[-1][1]) / lows[-1][1] > threshold:
                lows.append((i, l))

    return highs, lows


def contractions(highs, lows):
    drops = []
    for i in range(min(len(highs), len(lows)) - 1):
        h = highs[i][1]
        l = lows[i][1]
        if h > 0:
            drops.append((h - l) / h * 100)
    return drops


def valid_vcp(drops):
    if len(drops) < 3:
        return False

    if not (drops[0] > drops[1] > drops[2]):
        return False

    if drops[0] < 10:
        return False

    if drops[-1] > 10:
        return False

    return True


def volume_dryup(df):
    recent = df.tail(10)["volume"].mean()
    prior = df.tail(40).head(30)["volume"].mean()
    return recent < prior * 0.7


def breakout_volume(df):
    return df["volume"].iloc[-1] > df["volume"].tail(20).mean() * 1.5


def pivot_high(df):
    return df["high"].tail(20).max()


def near_pivot(df):
    p = pivot_high(df)
    return df["close"].iloc[-1] >= p * 0.97


def tight_range(df):
    r = df.tail(5)
    return (r["high"].max() - r["low"].min()) / r["high"].max() < 0.05


def score(row, drops):
    s = 50
    s += max(0, 20 - drops[-1])
    if row["pct_from_high"] >= -5:
        s += 10
    if row["pct_from_low"] > 50:
        s += 10
    if row["rs_rating"] >= 90:
        s += 10
    return min(s, 100)


# ================= ENGINE =================

def run():
    universe = fetch_universe()
    if universe.empty:
        return

    latest_date = get_latest_date()

    tickers = universe["ticker"].dropna().unique().tolist()
    price_df = fetch_price_data(tickers, latest_date)
    ind_df = fetch_indicators()

    price_df = price_df.sort_values(["ticker", "date"])
    grouped = dict(tuple(price_df.groupby("ticker")))

    universe = universe.merge(ind_df, on=["ticker", "exchange"], how="left")

    results = []

    for _, row in universe.iterrows():
        ticker = row["ticker"]

        if ticker not in grouped:
            continue

        df_full = grouped[ticker]

        if len(df_full) < 120:
            continue

        df = df_full.tail(PROCESS_WINDOW)

        # Filters
        if row["rs_rating"] < 80:
            continue

        if not prior_uptrend(df_full):
            continue

        if row["pct_from_high"] < -15:
            continue

        if not ma_alignment(row):
            continue

        highs, lows = find_swings(df)
        drops = contractions(highs, lows)

        if not valid_vcp(drops):
            continue

        if not volume_dryup(df):
            continue

        if not tight_range(df):
            continue

        pivot = pivot_high(df)
        close = df["close"].iloc[-1]

        status = "VCP"

        if near_pivot(df):
            status = "NEAR_PIVOT"

        if breakout_volume(df):
            status = "BREAKOUT_READY"

        if close > pivot:
            status = "BREAKOUT_CONFIRMED"

        s = score(row, drops)

        results.append(sanitize({
            "ticker": ticker,
            "exchange": row["exchange"],
            "vcp_score": int(s),
            "stage": status,
            "contractions": len(drops),
            "pattern": " → ".join([f"{round(d,1)}%" for d in drops[:4]]),
            "pivot": float(pivot),
            "pct_from_high": float(row["pct_from_high"]),
            "rs_rating": int(row["rs_rating"]),
            "detected_at": datetime.utcnow().isoformat()
        }))

    # overwrite table
    supabase.table("vcp_candidates").delete().neq("ticker", "").execute()

    if results:
        supabase.table("vcp_candidates").insert(results).execute()

    print(f"Stored {len(results)} VCP candidates")


if __name__ == "__main__":
    run()
