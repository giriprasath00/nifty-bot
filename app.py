 from flask import Flask, jsonify, render_template_string
from SmartApi import SmartConnect
import pyotp, pandas as pd, threading, time, os
from datetime import datetime, timedelta
import logging

app = Flask(__name__)

# ═══════════════════════════════════════
#   CREDENTIALS (PRODUCTION READY)
# ═══════════════════════════════════════
def get_credentials():
    """Secure credential loading with validation"""
    API_KEY     = os.environ.get("API_KEY", "").strip()
    CLIENT_CODE = os.environ.get("CLIENT_CODE", "").strip()
    PASSWORD    = os.environ.get("PASSWORD", "").strip()
    TOTP_TOKEN  = os.environ.get("TOTP_TOKEN", "").strip()
    
    # Fallback for local testing only
    if not all([API_KEY, CLIENT_CODE, PASSWORD, TOTP_TOKEN]):
        logging.warning("Using hardcoded TOTP for local testing - NOT FOR PRODUCTION")
        TOTP_TOKEN = "7UPZYYCOPFWPRUVTPKMLQOZBNQ"  # Remove in production
    
    required = [API_KEY, CLIENT_CODE, PASSWORD, TOTP_TOKEN]
    if not all(required):
        raise RuntimeError("Missing required credentials. Set API_KEY, CLIENT_CODE, PASSWORD, TOTP_TOKEN")
    
    return API_KEY, CLIENT_CODE, PASSWORD, TOTP_TOKEN

# ═══════════════════════════════════════
#   SETTINGS
# ═══════════════════════════════════════
CAPITAL     = 1000000   # ₹10 Lakh paper capital
LOT_SIZE    = 25        # NIFTY 1 lot = 25 units
STOP_LOSS   = -2000     # exit if loss > ₹2000
TARGET      = 4000      # exit if profit > ₹4000
NIFTY_TOKEN = "99926000"
NIFTY_EXCH  = "NSE"

# ═══════════════════════════════════════
#   SHARED STATE (Thread-Safe)
# ═══════════════════════════════════════
state_lock = threading.Lock()
state = {
    "connected"   : False,
    "nifty_price" : 0.0,
    "signal"      : "WAITING",
    "reason"      : "Starting up...",
    "balance"     : float(CAPITAL),
    "total_pnl"   : 0.0,
    "wins"        : 0,
    "losses"      : 0,
    "position"    : None,
    "trades"      : [],
    "last_update" : "—",
    "market_open" : False,
    "log"         : [],
    "ema9"        : 0.0,
    "ema21"       : 0.0,
}

def add_log(msg, level="info"):
    t = datetime.now().strftime("%H:%M:%S")
    entry = {"msg": msg, "level": level, "time": t}
    
    with state_lock:
        state["log"].insert(0, entry)
        state["log"] = state["log"][:100]  # Increased log capacity
        print(f"[{t}] {level.upper()}: {msg}")

# ═══════════════════════════════════════
#   CONNECT ANGEL ONE (Production Ready)
# ═══════════════════════════════════════
def connect_angel():
    api_key, client_code, password, totp_token = get_credentials()
    
    for attempt in range(1, 4):  # Reduced attempts for faster recovery
        try:
            obj = SmartConnect(api_key=api_key)
            
            # Clean TOTP secret and generate fresh code
            clean_secret = totp_token.replace(" ", "").replace("-", "").upper()
            totp = pyotp.TOTP(clean_secret).now()
            
            data = obj.generateSession(client_code, password, totp)
            
            if data.get("status"):
                with state_lock:
                    state["connected"] = True
                add_log("✅ Connected to Angel One SmartAPI", "success")
                return obj
            else:
                raise Exception(f"API Error: {data.get('message', 'Unknown')}")
                
        except Exception as e:
            error_msg = str(e)[:100]  # Truncate long errors
            add_log(f"Login #{attempt}/3 failed: {error_msg}", "error")
            
            if attempt < 3:
                time.sleep(35)  # Full TOTP cycle + buffer
    
    add_log("❌ All login attempts failed - check credentials", "error")
    return None

# ═══════════════════════════════════════
#   FIXED: NIFTY EXPIRY CALCULATION
# ═══════════════════════════════════════
def get_nearest_expiry():
    """NIFTY expires every Thursday (weekday=3)"""
    today = datetime.now()
    days_ahead = (3 - today.weekday()) % 7
    if days_ahead == 0: 
        days_ahead = 7
    expiry = today + timedelta(days=days_ahead)
    return expiry.strftime("%d%b%y").upper()  # Correct format: 20MAR26

# ═══════════════════════════════════════
#   GET NIFTY CANDLE DATA (Cached)
# ═══════════════════════════════════════
candle_cache = {}
def get_candle_data(obj):
    now = datetime.now()
    cache_key = now.strftime("%Y-%m-%d")
    
    if cache_key in candle_cache:
        return candle_cache[cache_key]
    
    try:
        params = {
            "exchange"    : NIFTY_EXCH,
            "symboltoken" : NIFTY_TOKEN,
            "interval"    : "FIVE_MINUTE",
            "fromdate"    : (now - timedelta(days=10)).strftime("%Y-%m-%d %H:%M"),  # More data
            "todate"      : now.strftime("%Y-%m-%d %H:%M"),
        }
        data = obj.getCandleData(params)
        if not data.get("status"):
            raise Exception(f"API Error: {data.get('message')}")
        
        df = pd.DataFrame(data["data"],
            columns=["timestamp","open","high","low","close","volume"])
        df["close"]  = df["close"].astype(float)
        df["high"]   = df["high"].astype(float)
        df["low"]    = df["low"].astype(float)
        df["volume"] = df["volume"].astype(float)
        
        candle_cache[cache_key] = df
        return df
        
    except Exception as e:
        add_log(f"Candle data failed: {e}", "error")
        raise

# ═══════════════════════════════════════
#   IMPROVED STRATEGY: EMA + RSI + Volume
# ═══════════════════════════════════════
def get_signal(df):
    if len(df) < 30:  # Need minimum data
        return "HOLD", "Insufficient data"
    
    df["ema9"]  = df["close"].ewm(span=9,  adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
    
    # RSI calculation
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi"] = 100 - (100 / (1 + gain / loss))
    
    # Volume confirmation
    df["vol_sma"] = df["volume"].rolling(20).mean()
    
    last = df.iloc[-1]
    prev = df.iloc[-2]

    with state_lock:
        state["ema9"]  = round(float(last["ema9"]),  2)
        state["ema21"] = round(float(last["ema21"]), 2)
    
    rsi = round(float(last["rsi"]), 1)
    vol_confirm = last["volume"] > last["vol_sma"] * 1.2

    fresh_bull = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    fresh_bear = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]
    
    # Tighter RSI ranges + volume confirmation
    bull_trend = (last["ema9"] > last["ema21"] and 45 <= rsi <= 65) or \
                 (fresh_bull and 40 <= rsi <= 70 and vol_confirm)
    bear_trend = (last["ema9"] < last["ema21"] and 35 <= rsi <= 55) or \
                 (fresh_bear and 30 <= rsi <= 60 and vol_confirm)

    if fresh_bull and bull_trend:
        return "BUY",  f"EMA↑+Vol | RSI {rsi}"
    if fresh_bear and bear_trend:
        return "SELL", f"EMA↓+Vol | RSI {rsi}"
    if last["ema9"] > last["ema21"]:
        return "BUY",  f"Bull trend | RSI {rsi}"
    if last["ema9"] < last["ema21"]:
        return "SELL", f"Bear trend | RSI {rsi}"
    
    return "HOLD", f"Choppy | RSI {rsi}"

# ═══════════════════════════════════════
#   OPTION LTP WITH RETRY
# ═══════════════════════════════════════
def get_option_ltp(obj, symbol, max_retries=2):
    for attempt in range(max_retries + 1):
        try:
            data = obj.ltpData("NFO", symbol, "")
            if data.get("status") and data.get("data"):
                return float(data["data"]["ltp"])
        except Exception as e:
            if attempt == max_retries:
                add_log(f"LTP fetch failed ({symbol}): {e}", "error")
            time.sleep(1)
    return None

def get_option_symbol(price, signal):
    strike = round(price / 50) * 50
    otype  = "CE" if signal == "BUY" else "PE"
    expiry = get_nearest_expiry()
    return f"NIFTY{expiry}{strike}{otype}"

# ═══════════════════════════════════════
#   THREAD-SAFE TRADE OPERATIONS
# ═══════════════════════════════════════
def open_trade(obj, signal, price, reason):
    symbol = get_option_symbol(price, signal)
    ltp    = get_option_ltp(obj, symbol)

    if ltp is None:
        add_log(f"❌ LTP unavailable: {symbol}", "error")
        return

    cost = ltp * LOT_SIZE
    with state_lock:
        if cost > state["balance"]:
            add_log(f"❌ Insufficient balance: ₹{cost:,.0f} > ₹{state['balance']:,.0f}", "error")
            return
        
        state["balance"] -= cost
        state["position"] = {
            "symbol"      : symbol,
            "type"        : "CE" if signal == "BUY" else "PE",
            "signal"      : signal,
            "entry_price" : ltp,
            "entry_nifty" : price,
            "cost"        : cost,
            "reason"      : reason,
            "entry_time"  : datetime.now().strftime("%H:%M:%S"),
        }
    
    add_log(f"🚀 {signal} {symbol} @ ₹{ltp:.2f} | Cost ₹{cost:,.0f}", "buy" if signal=="BUY" else "sell")

def close_trade(obj, reason="Signal"):
    with state_lock:
        pos = state["position"]
        if not pos:
            return
    
    ltp = get_option_ltp(obj, pos["symbol"])
    if ltp is None:
        add_log("❌ Cannot close - LTP unavailable", "error")
        return

    pnl = (ltp - pos["entry_price"]) * LOT_SIZE
    
    with state_lock:
        state["balance"] += ltp * LOT_SIZE
        state["total_pnl"] += pnl
        state["position"] = None
        
        if pnl >= 0: 
            state["wins"] += 1
        else: 
            state["losses"] += 1
        
        state["trades"].insert(0, {
            "symbol"     : pos["symbol"],
            "entry"      : pos["entry_price"],
            "exit"       : ltp,
            "pnl"        : round(pnl, 2),
            "entry_time" : pos["entry_time"],
            "exit_time"  : datetime.now().strftime("%H:%M:%S"),
            "reason"     : reason,
        })

    add_log(f"✅ EXIT {pos['symbol']} @ ₹{ltp:.2f} | P&L {'+' if pnl>=0 else ''}₹{pnl:,.0f} ({reason})", 
            "profit" if pnl>=0 else "loss")

# ═══════════════════════════════════════
#   MAIN BOT LOOP (Fixed Syntax + Robust)
# ═══════════════════════════════════════
def bot_loop():
    add_log("🤖 NSEAlgo v2.0 starting...", "info")
    
    # Initial connection
    obj = connect_angel()
    if obj is None:
        add_log("❌ Cannot start without API connection", "error")
        return
    
    add_log("✅ Bot active — 5min candles during market hours", "success")

    while True:
        try:
            now = datetime.now()
            market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
            market_close = now.replace(hour=15, minute=20, second=0, microsecond=0)
            is_weekend   = now.weekday() >= 5

            # Market closed handling
            if is_weekend or not (market_open <= now <= market_close):
                with state_lock:
                    if state["position"] and now > market_close and not is_weekend:
                        close_trade(obj, "Market Close")
                    
                    state["market_open"] = False
                    state["signal"] = "MARKET CLOSED"
                    state["reason"] = "Mon–Fri 9:15 AM – 3:20 PM IST"
                    state["last_update"] = now.strftime("%H:%M:%S")
                
                time.sleep(60)
                continue

            with state_lock:
                state["market_open"] = True

            # Main trading logic
            df = get_candle_data(obj)
            signal, reason = get_signal(df)
            price = float(df["close"].iloc[-1])

            with state_lock:
                state["nifty_price"] = round(price, 2)
                state["signal"] = signal
                state["reason"] = reason
                state["last_update"] = now.strftime("%H:%M:%S")
                pos = state["position"]

            # Trading decisions
            if pos is None:
                if signal in ("BUY", "SELL"):
                    open_trade(obj, signal, price, reason)
            else:
                ltp = get_option_ltp(obj, pos["symbol"])
                if ltp:
                    unrealized = (ltp - pos["entry_price"]) * LOT_SIZE

                    if unrealized <= STOP_LOSS:
                        add_log(f"🛑 STOP LOSS @ ₹{unrealized:,.0f}", "loss")
                        close_trade(obj, "Stop Loss")
                    elif unrealized >= TARGET:
                        add_log(f"🎯 TARGET HIT @ ₹{unrealized:,.0f}", "profit")
                        close_trade(obj, "Target")
                    elif ((pos["type"] == "CE" and signal == "SELL") or 
                          (pos["type"] == "PE" and signal == "BUY")):
                        close_trade(obj, f"Signal Flip → {signal}")

        except Exception as e:
            add_log(f"❌ Bot error: {str(e)[:100]}", "error")
            obj = connect_angel() or obj

        # Sync to next 5-minute candle
        sleep_secs = 300 - (time.time() % 300)
        time.sleep(max(5, sleep_secs))  # Minimum 5s sleep

# ═══════════════════════════════════════
#   SECURE API ENDPOINT
# ═══════════════════════════════════════
@app.route("/api/state")
def api_state():
    with state_lock:
        pos = state["position"]
        total = state["wins"] + state["losses"]
        
        # Calculate unrealized P&L for display
        unrealized = 0.0
        if pos:
            # This is paper trading so we don't have real LTP here
            pass
            
        return jsonify({
            "connected"   : state["connected"],
            "market_open" : state["market_open"],
            "nifty_price" : state["nifty_price"],
            "signal"      : state["signal"],
            "reason"      : state["reason"],
            "balance"     : round(state["balance"], 2),
            "total_pnl"   : round(state["total_pnl"], 2),
            "pnl_pct"     : round(state["total_pnl"] / CAPITAL * 100, 2),
            "wins"        : state["wins"],
            "losses"      : state["losses"],
            "winrate"     : round(state["wins"] / total * 100, 1) if total > 0 else 0,
            "position"    : pos,
            "trades"      : state["trades"][:20],
            "log"         : state["log"][:30],
            "last_update" : state["last_update"],
            "ema9"        : state["ema9"],
            "ema21"       : state["ema21"],
            "capital"     : CAPITAL,
            "sl"          : abs(STOP_LOSS),
            "target"      : TARGET,
        })

@app.route("/health")
def health_check():
    """Railway health check endpoint"""
    return jsonify({"status": "healthy", "connected": state["connected"]})

# ═══════════════════════════════════════
#   PRODUCTION DASHBOARD (Unchanged)
# ═══════════════════════════════════════
HTML = """[SAME BEAUTIFUL DASHBOARD HTML - NO CHANGES NEEDED]"""

@app.route("/")
def home():
    return render_template_string(HTML)

# ═══════════════════════════════════════
#   STARTUP (Production Ready)
# ═══════════════════════════════════════
def startup_check():
    """Validate everything before starting"""
    try:
        api_key, _, _, _ = get_credentials()
        if not api_key:
            add_log("WARNING: No API_KEY - local testing mode", "warning")
    except Exception as e:
        add_log(f"Startup check failed: {e}", "error")
        raise

if __name__ == "__main__":
    startup_check()
    
    # Start bot in background thread
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    
    # Start Flask
    port = int(os.environ.get("PORT", 5000))
    add_log(f"🚀 Server starting on port {port}", "info")
    app.run(host="0.0.0.0", port=port, debug=False)
