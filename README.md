# 📊 VCP Screener Engine (TradeEdge)

A production-grade **Volatility Contraction Pattern (VCP) detection engine** built for Indian equities, inspired by Mark Minervini’s methodology.

This system scans the market daily, identifies high-quality VCP setups, scores them, and stores results for UI consumption.

---

## 🚀 Features

* ✅ Stage 2 trend filtering (SMA alignment)
* ✅ Multi-leg volatility contraction detection
* ✅ Volume dry-up analysis
* ✅ Tight price range detection
* ✅ Pivot proximity (52-week high)
* ✅ Scoring & ranking system
* ✅ Supabase integration (Postgres)
* ✅ Automated daily execution via GitHub Actions

---

## 🧠 Strategy Overview

The engine identifies **VCP setups**, characterized by:

* Prior uptrend (price > SMA50 > SMA200)
* Sequential contraction in price volatility
* Declining volume across contractions
* Tight consolidation near highs
* Breakout readiness

---

## 🏗️ Architecture

```
stock_prices_daily   → raw OHLCV data
stock_52w            → precomputed indicators (trend + positioning)
        ↓
vcp_engine.py        → detection + scoring logic
        ↓
vcp_candidates       → stored results
        ↓
React UI             → display (Technical Screens → VCP)
```

---

## 🗄️ Database Schema

### Source Tables

#### `stock_prices_daily`

* ticker, exchange, date
* open, high, low, close
* volume

#### `stock_52w`

* high_52w, low_52w
* pct_from_high, pct_from_low
* sma50, sma200
* volume, volume_ma20

---

### Output Table

#### `vcp_candidates`

```sql
CREATE TABLE vcp_candidates (
  date DATE,
  ticker TEXT,
  exchange TEXT,

  vcp_score INT,
  contractions INT,
  contraction_pattern TEXT,

  pct_from_high NUMERIC,
  base_depth NUMERIC,

  volume_ratio NUMERIC,
  volume_dryup BOOLEAN,

  tight_range BOOLEAN,
  near_pivot BOOLEAN,

  breakout_level NUMERIC,

  created_at TIMESTAMP DEFAULT NOW(),

  PRIMARY KEY (date, ticker)
);
```

---

## ⚙️ Setup Instructions

### 1. Clone Repository

```
git clone https://github.com/<your-username>/tradeedge-vcp-engine.git
cd tradeedge-vcp-engine
```

---

### 2. Install Dependencies

```
pip install -r requirements.txt
```

---

### 3. Configure Environment Variables

Set the following in your environment or GitHub Secrets:

* `SUPABASE_URL`
* `SUPABASE_KEY` (use service_role key)

---

### 4. Run Locally

```
python vcp_engine.py
```

---

## ⏱️ Automation (GitHub Actions)

The engine runs automatically every trading day.

### Schedule:

* **20:00 IST (14:30 UTC)**
* After data ingestion and indicator updates

### Workflow File:

```
.github/workflows/vcp.yml
```

---

## 📊 Output Example

Each detected VCP setup includes:

```
Ticker: ABC

Score: 82
Contractions: 28% → 14% → 6%

Position: -4.2% from 52W High
Volume: 0.58x avg (dry-up)

Tight Range: Yes
Breakout Level: ₹523
```

---

## 🧮 Scoring Model

| Factor                | Weight |
| --------------------- | ------ |
| Trend Alignment       | 20     |
| Proximity to High     | 20     |
| Contraction Structure | 25     |
| Volume Dry-Up         | 15     |
| Tight Range           | 10     |
| Prior Move Strength   | 10     |

---

## 📌 Classification

* **80+ → Ideal VCP**
* **60–80 → Developing Setup**
* **<60 → Ignore**

---

## ⚠️ Notes

* Requires minimum ~100 days of price data per stock
* Works best in trending markets
* Initial run may take longer due to data fetching

---

## 🚀 Future Enhancements

* 📈 Relative Strength (RS) vs Nifty 500
* 📊 Mini chart with VCP overlay
* 🔁 Setup persistence tracking (multi-day)
* 🧪 Backtesting engine for breakout success
* ⚡ Performance optimization (batch + parallel processing)

---

## 🛠️ Tech Stack

* Python (Pandas, NumPy)
* Supabase (Postgres)
* GitHub Actions (cron automation)
* React (frontend UI)

---

## 🤝 Contribution

This is part of the **TradeEdge** ecosystem. Contributions and enhancements are welcome.

---

## 📄 License

Private / Proprietary (or update as needed)

---

## 💡 Disclaimer

This tool is for **educational and research purposes only**.
Not financial advice. Always perform your own due diligence.

---
