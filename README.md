# 📈 NSEAlgo v4 - Institutional Options Trading Bot

NSEAlgo v4 is a fully automated, full-stack NIFTY 50 options trading bot built with Python, Flask, and AngelOne's SmartAPI. It features a real-time WebSocket dashboard and executes trades using a 2/3 majority voting algorithm based on EMA, Central Pivot Range (CPR), and dynamic 3-Touch Power Support & Resistance zones.

## 🚀 Key Features

* **Smart Strategy Engine (2/3 Consensus):** * **EMA 9/21 Cross:** Calculates trend direction using 15-second micro-candles.
  * **Central Pivot Range (CPR):** Fetches historical daily candles to calculate the Previous Day High/Low (PDH/PDL) and CPR pivot for daily trend bias.
  * **Power S&R Zones:** Uses a custom clustering algorithm to track 5-minute swing highs/lows. A Support/Resistance zone is only validated if it receives at least 3 distinct touches (institutional order block tracking).
* **Dynamic Trailing Risk Management:** Automatically trails the Stop Loss behind winning trades and dynamically extends the Target Profit to let winners run.
* **Zero-Lag WebSocket Integration:** Uses `SmartWebSocketV2` for live sub-second tick processing without API rate limits.
* **Interactive Light Mode Dashboard:** A Flask-served HTML/JS frontend displaying live P&L, signal voting status, real-time indicators, and active position metrics.
* **Manual Override & Control:** 1-click manual CE/PE buy buttons, position closing, and a bot pause/resume toggle directly from the UI.
* **Automated Strike Selection:** Automatically calculates and selects the nearest ATM (At-The-Money) NIFTY weekly expiry strike.

## 🛠️ Tech Stack
* **Backend:** Python 3, Flask, threading, Pandas, NumPy, AngelOne SmartAPI
* **Frontend:** HTML5, CSS3 (Custom Light Mode UI), Vanilla JavaScript
* **Deployment Ready:** Configured for local hosting or cloud deployment (e.g., Render, Railway) via `os.environ.get("PORT", 5000)`.
 

 
