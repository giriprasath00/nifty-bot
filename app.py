from flask import Flask, jsonify, render_template_string
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp, pandas as pd, threading, time, os, requests, json
from datetime import datetime, timedelta
from collections import deque
import numpy as np
import pytz

IST = pytz.timezone("Asia/Kolkata")
app = Flask(__name__)

# ═══════════════════════════════════════
#   CREDENTIALS
# ═══════════════════════════════════════
API_KEY     = os.environ.get("API_KEY",     "")
CLIENT_CODE = os.environ.get("CLIENT_CODE", "")
PASSWORD    = os.environ.get("PASSWORD",    "")
TOTP_TOKEN  = os.environ.get("TOTP_TOKEN",  "")

# ═══════════════════════════════════════
#   SETTINGS
# ═══════════════════════════════════════
CAPITAL          = 1000000
LOT_SIZE         = 65
STOP_LOSS_PCT    = 0.15
TARGET_PCT       = 0.45
CAPITAL_USE_PCT  = 0.80
NIFTY_TOKEN      = "99926000"
NIFTY_EXCH       = "NSE"
NIFTY_EXCH_TYPE  = 1        # 1 = NSE for WebSocket

# Trading sessions IST
SESSION_1 = ((9, 30), (11, 30))
SESSION_2 = ((13, 30), (15, 0))

# ═══════════════════════════════════════
#   TICK BUFFERS
# ═══════════════════════════════════════
tick_buffer    = deque(maxlen=15)    # 1-sec ticks → 15 sec window
micro_buffer   = deque(maxlen=12)    # 15-sec avgs → 3 min window
hist_buffer    = deque(maxlen=500)   # long history for S&R + ADX

last_micro_time  = [time.time()]
last_signal_time = [time.time()]

# ═══════════════════════════════════════
#   SHARED STATE
# ═══════════════════════════════════════
state = {
    "connected"    : False,
    "ws_connected" : False,
    "nifty_price"  : 0.0,
    "signal"       : "WAITING",
    "reason"       : "Connecting...",
    "balance"      : float(CAPITAL),
    "total_pnl"    : 0.0,
    "wins"         : 0,
    "losses"       : 0,
    "position"     : None,
    "trades"       : [],
    "last_update"  : "—",
    "market_open"  : False,
    "log"          : [],
    "ema9"         : 0.0,
    "ema21"        : 0.0,
    "vwap"         : 0.0,
    "adx"          : 0.0,
    "support"      : 0.0,
    "resistance"   : 0.0,
    "ema_signal"   : "—",
    "vwap_signal"  : "—",
    "sr_signal"    : "—",
    "adx_value"    : 0.0,
    "buy_votes"    : 0,
    "sell_votes"   : 0,
    "in_session"   : False,
    "tick_count"   : 0,
    "micro_count"  : 0,
    "vol_ratio"    : 0.0,
}

def add_log(msg, level="info"):
    t = datetime.now(IST).strftime("%H:%M:%S")
    state["log"].insert(0, {"msg": msg, "level": level, "time": t})
    state["log"] = state["log"][:60]
    print(f"[{t}] {msg}")

# ═══════════════════════════════════════
#   SYMBOL MASTER
# ═══════════════════════════════════════
nfo_symbols = {}

def load_symbol_master():
    global nfo_symbols
    try:
        url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        data = requests.get(url, timeout=15).json()
        for item in data:
            if item.get("exch_seg") == "NFO" and "NIFTY" in item.get("symbol", ""):
                nfo_symbols[item["symbol"]] = item["token"]
        add_log(f"Loaded {len(nfo_symbols)} NIFTY NFO symbols ✓", "success")
    except Exception as e:
        add_log(f"Symbol master failed: {e}", "error")

def get_option_token(symbol):
    return nfo_symbols.get(symbol)

# ═══════════════════════════════════════
#   CONNECT ANGEL ONE
# ═══════════════════════════════════════
_obj = [None]
_auth_token = [""]
_feed_token = [""]

def connect_angel():
    for attempt in range(1, 6):
        try:
            obj  = SmartConnect(api_key=API_KEY.strip())
            totp = pyotp.TOTP(TOTP_TOKEN.strip()).now()
            data = obj.generateSession(CLIENT_CODE.strip(), PASSWORD.strip(), totp)
            if not data.get("status"):
                raise Exception(f"Login failed: {data.get('message')}")
            _obj[0]        = obj
            _auth_token[0] = data["data"]["jwtToken"]
            _feed_token[0] = obj.getfeedToken()
            state["connected"] = True
            add_log("Connected to Angel One ✓", "success")
            return obj
        except Exception as e:
            add_log(f"Login attempt {attempt}/5: {e}", "error")
            time.sleep(15)
    return None

# ═══════════════════════════════════════
#   WEBSOCKET HANDLERS
# ═══════════════════════════════════════
def on_data(wsapp, message):
    try:
        if not isinstance(message, dict):
            return
       # print first tick to see raw values
        if state["tick_count"] == 0:
            add_log(f"Raw tick: {message}", "info")

        raw    = message.get("last_traded_price", 0)
        ltp    = raw / 100.0 if raw > 100000 else float(raw)
        volume = message.get("volume_trade_for_the_day",
                 message.get("vol_traded_today",
                 message.get("volume", 0)))
        if ltp <= 0:
            return
        now = datetime.now(IST).replace(tzinfo=None)
        tick = {"price": ltp, "volume": volume, "time": now}
        tick_buffer.append(tick)
        hist_buffer.append(tick)

        state["nifty_price"] = round(ltp, 2)
        state["tick_count"]  += 1
        state["last_update"]  = now.strftime("%H:%M:%S")

        # every 15 seconds → compute micro average
        if time.time() - last_micro_time[0] >= 15:
            compute_micro_average()
            last_micro_time[0] = time.time()

        # every 3 minutes → compute signal and trade
        if time.time() - last_signal_time[0] >= 180:
            compute_signal_and_trade()
            last_signal_time[0] = time.time()

    except Exception as e:
        pass

def on_open(wsapp):
    state["ws_connected"] = True
    add_log("WebSocket connected — receiving live ticks ✓", "success")

def on_error(wsapp, error):
    state["ws_connected"] = False
    add_log(f"WebSocket error: {error}", "error")

def on_close(wsapp):
    state["ws_connected"] = False
    add_log("WebSocket disconnected", "error")

# ═══════════════════════════════════════
#   MICRO AVERAGE (every 15 seconds)
# ═══════════════════════════════════════
def compute_micro_average():
    if len(tick_buffer) < 3:
        return
    prices  = [t["price"]  for t in tick_buffer]
    volumes = [t["volume"] for t in tick_buffer]
    micro = {
        "avg_price"  : np.mean(prices),
        "high_price" : max(prices),
        "low_price"  : min(prices),
        "avg_volume" : np.mean(volumes),
        "max_volume" : max(volumes),
        "time"       : datetime.now(IST).replace(tzinfo=None),
    }
    micro_buffer.append(micro)
    state["micro_count"] += 1
    adx = calc_adx()
    state["adx_value"] = adx

# ═══════════════════════════════════════
#   VWAP CALCULATION
# ═══════════════════════════════════════
def calc_vwap():
    if len(hist_buffer) < 10:
        return 0.0
    prices  = np.array([t["price"]  for t in hist_buffer])
    volumes = np.array([t["volume"] for t in hist_buffer])
    # avoid division by zero
    total_vol = np.sum(volumes)
    if total_vol == 0:
        return float(np.mean(prices))
    vwap = np.sum(prices * volumes) / total_vol
    return round(float(vwap), 2)

# ═══════════════════════════════════════
#   ADX CALCULATION (trend strength)
# ═══════════════════════════════════════
def calc_adx(period=14):
    if len(micro_buffer) < period + 1:
        return 0.0
    highs  = np.array([m["high_price"] for m in micro_buffer])
    lows   = np.array([m["low_price"]  for m in micro_buffer])
    closes = np.array([m["avg_price"]  for m in micro_buffer])

    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, len(closes)):
        tr  = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        pdm = max(highs[i]-highs[i-1], 0) if (highs[i]-highs[i-1]) > (lows[i-1]-lows[i]) else 0
        ndm = max(lows[i-1]-lows[i], 0)   if (lows[i-1]-lows[i]) > (highs[i]-highs[i-1]) else 0
        tr_list.append(tr); pdm_list.append(pdm); ndm_list.append(ndm)

    if len(tr_list) < period:
        return 0.0

    atr  = np.mean(tr_list[-period:])
    if atr == 0:
        return 0.0
    pdi  = 100 * np.mean(pdm_list[-period:]) / atr
    ndi  = 100 * np.mean(ndm_list[-period:]) / atr
    dx   = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) > 0 else 0
    return round(float(dx), 1)

# ═══════════════════════════════════════
#   EMA on micro averages
# ═══════════════════════════════════════
def calc_ema_micro():
    if len(micro_buffer) < 9:
        return 0.0, 0.0, "NEUTRAL"
    prices = pd.Series([m["avg_price"] for m in micro_buffer])
    ema9   = float(prices.ewm(span=9,  adjust=False).mean().iloc[-1])
    ema21  = float(prices.ewm(span=21, adjust=False).mean().iloc[-1]) if len(prices) >= 21 else ema9
    state["ema9"]  = round(ema9, 2)
    state["ema21"] = round(ema21, 2)
    if ema9 > ema21:
        state["ema_signal"] = f"BUY — EMA9 {ema9:.0f} > EMA21 {ema21:.0f}"
        return ema9, ema21, "BUY"
    else:
        state["ema_signal"] = f"SELL — EMA9 {ema9:.0f} < EMA21 {ema21:.0f}"
        return ema9, ema21, "SELL"

# ═══════════════════════════════════════
#   VWAP SIGNAL
# ═══════════════════════════════════════
def calc_vwap_signal():
    vwap  = calc_vwap()
    price = state["nifty_price"]
    state["vwap"] = vwap
    if vwap == 0:
        state["vwap_signal"] = "NEUTRAL — VWAP calculating"
        return "NEUTRAL"
    if price > vwap * 1.001:
        state["vwap_signal"] = f"BUY — Price ₹{price:.0f} above VWAP ₹{vwap:.0f}"
        return "BUY"
    if price < vwap * 0.999:
        state["vwap_signal"] = f"SELL — Price ₹{price:.0f} below VWAP ₹{vwap:.0f}"
        return "SELL"
    state["vwap_signal"] = f"NEUTRAL — At VWAP ₹{vwap:.0f}"
    return "NEUTRAL"

# ═══════════════════════════════════════
#   SUPPORT & RESISTANCE
# ═══════════════════════════════════════
def calc_sr_signal():
    if len(micro_buffer) < 5:
        state["sr_signal"] = "NEUTRAL — Not enough data"
        return "NEUTRAL"
    prices     = [m["avg_price"] for m in micro_buffer]
    resistance = round(max(prices), 2)
    support    = round(min(prices), 2)
    price      = state["nifty_price"]
    buffer     = (resistance - support) * 0.015
    state["support"]    = support
    state["resistance"] = resistance
    if price >= resistance - buffer:
        state["sr_signal"] = f"SELL — Near resistance ₹{resistance:.0f}"
        return "SELL"
    if price <= support + buffer:
        state["sr_signal"] = f"BUY — Near support ₹{support:.0f}"
        return "BUY"
    mid = (resistance + support) / 2
    if price > mid:
        state["sr_signal"] = f"BUY — Above midzone ₹{mid:.0f}"
        return "BUY"
    state["sr_signal"] = f"SELL — Below midzone ₹{mid:.0f}"
    return "SELL"

# ═══════════════════════════════════════
#   COMBINED SIGNAL (2 out of 3 + ADX filter)
# ═══════════════════════════════════════
def compute_signal_and_trade():
    try:
        now = datetime.now(IST).replace(tzinfo=None)

        # check market hours
        market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=20, second=0, microsecond=0)
        if not (market_open <= now <= market_close) or now.weekday() >= 5:
            state["signal"] = "MARKET CLOSED"
            state["reason"] = "Mon–Fri 9:15 AM – 3:20 PM IST"
            return

        state["market_open"] = True

        state["in_session"] = True

        if len(micro_buffer) < 5:
            state["signal"] = "HOLD"
            state["reason"] = f"Building data... {len(micro_buffer)}/12 micro candles"
            return

        # calculate all signals
        _, _, ema_s  = calc_ema_micro()
        vwap_s       = calc_vwap_signal()
        sr_s         = calc_sr_signal()
        adx          = calc_adx()
        state["adx_value"] = adx

        buy_votes  = sum(1 for s in [ema_s, vwap_s, sr_s] if s == "BUY")
        sell_votes = sum(1 for s in [ema_s, vwap_s, sr_s] if s == "SELL")
        state["buy_votes"]  = buy_votes
        state["sell_votes"] = sell_votes

        # ADX filter — only trade in trending market
        trend_strong = adx >= 25

        if buy_votes >= 2 and trend_strong:
            signal = "BUY"
            reason = f"{buy_votes}/3 agree BUY | ADX {adx:.1f}"
        elif sell_votes >= 2 and trend_strong:
            signal = "SELL"
            reason = f"{sell_votes}/3 agree SELL | ADX {adx:.1f}"
        elif not trend_strong:
            signal = "HOLD"
            reason = f"Choppy market — ADX {adx:.1f} < 15 (need trend)"
        else:
            signal = "HOLD"
            reason = f"No consensus | BUY {buy_votes}/3 SELL {sell_votes}/3"

        state["signal"] = signal
        state["reason"] = reason

        # execute trade
        obj = _obj[0]
        if obj is None:
            return

        pos = state["position"]
        price = state["nifty_price"]

        if pos is None:
           if signal in ("BUY", "SELL"):
               open_trade(obj, signal, price, reason)
        else:
            ltp = get_option_ltp(obj, pos["symbol"])
            if ltp:
                if ltp <= pos["sl_price"]:
                    add_log(f"Stop loss hit! ₹{ltp:.2f}", "loss")
                    close_trade(obj, "Stop Loss")
                elif ltp >= pos["tgt_price"]:
                    add_log(f"Target hit! ₹{ltp:.2f}", "profit")
                    close_trade(obj, "Target")
                elif (pos["type"] == "CE" and signal == "SELL") or \
                     (pos["type"] == "PE" and signal == "BUY"):
                    close_trade(obj, f"Signal flip → {signal}")
               
                 
    except Exception as e:
        add_log(f"Signal compute error: {e}", "error")

# ═══════════════════════════════════════
#   OPTION FUNCTIONS
# ═══════════════════════════════════════
def get_nearest_expiry():
    today      = datetime.now(IST).replace(tzinfo=None)
    days_ahead = (3 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    expiry = today + timedelta(days=days_ahead)
    return expiry.strftime("%d%b%y").upper()

def get_option_symbol(price, signal):
    strike = round(price / 50) * 50
    otype  = "CE" if signal == "BUY" else "PE"
    return f"NIFTY{get_nearest_expiry()}{strike}{otype}"

def get_option_ltp(obj, symbol):
    token = get_option_token(symbol)
    if not token:
        return None
    try:
        data    = obj.getMarketData("LTP", {"NFO": [token]})
        fetched = data.get("data", {}).get("fetched", [])
        if fetched:
            return float(fetched[0]["ltp"])
    except:
        pass
    return None

def open_trade(obj, signal, price, reason):
    symbol = get_option_symbol(price, signal)
    ltp    = get_option_ltp(obj, symbol)
    if ltp is None:
        add_log(f"LTP not found: {symbol}", "error")
        return
    max_spend = state["balance"] * CAPITAL_USE_PCT
    lots      = max(1, int(max_spend / (ltp * LOT_SIZE)))
    qty       = lots * LOT_SIZE
    cost      = ltp * qty
    if cost > state["balance"]:
        add_log("Insufficient balance!", "error")
        return
    sl_price  = round(ltp * (1 - STOP_LOSS_PCT), 2)
    tgt_price = round(ltp * (1 + TARGET_PCT),    2)
    state["balance"]  -= cost
    state["position"]  = {
        "symbol"      : symbol,
        "type"        : "CE" if signal == "BUY" else "PE",
        "signal"      : signal,
        "entry_price" : ltp,
        "entry_nifty" : price,
        "cost"        : cost,
        "lots"        : lots,
        "qty"         : qty,
        "sl_price"    : sl_price,
        "tgt_price"   : tgt_price,
        "reason"      : reason,
        "entry_time"  : datetime.now(IST).strftime("%H:%M:%S"),
    }
    add_log(f"{'BUY' if signal=='BUY' else 'SELL'} {symbol} @ ₹{ltp:.2f} | {lots} lots | SL ₹{sl_price} | TGT ₹{tgt_price}", "buy" if signal=="BUY" else "sell")

def close_trade(obj, reason="Signal"):
    pos = state["position"]
    if not pos:
        return
    ltp = get_option_ltp(obj, pos["symbol"])
    if ltp is None:
        add_log("Cannot close — LTP unavailable", "error")
        return
    qty = pos.get("qty", LOT_SIZE)
    pnl = (ltp - pos["entry_price"]) * qty
    state["balance"]   += ltp * qty
    state["total_pnl"] += pnl
    state["position"]   = None
    if pnl >= 0: state["wins"]   += 1
    else:        state["losses"] += 1
    state["trades"].insert(0, {
        "symbol"     : pos["symbol"],
        "entry"      : pos["entry_price"],
        "exit"       : ltp,
        "pnl"        : round(pnl, 2),
        "lots"       : pos["lots"],
        "entry_time" : pos["entry_time"],
        "exit_time"  : datetime.now(IST).strftime("%H:%M:%S"),
        "reason"     : reason,
    })
    add_log(f"EXIT {pos['symbol']} @ ₹{ltp:.2f} | P&L {'+' if pnl>=0 else ''}₹{pnl:,.0f} ({reason})", "profit" if pnl>=0 else "loss")

# ═══════════════════════════════════════
#   WEBSOCKET BOT LOOP
# ═══════════════════════════════════════
def bot_loop():
    add_log("NSEAlgo v4 — WebSocket starting...", "info")

    obj = connect_angel()
    if obj is None:
        add_log("Cannot start — login failed", "error")
        return
    load_symbol_master()

    while True:
        try:
            now = datetime.now(IST).replace(tzinfo=None)
            market_open  = now.replace(hour=9,  minute=0,  second=0, microsecond=0)
            market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
            is_weekend   = now.weekday() >= 5

            if is_weekend or not (market_open <= now <= market_close):
                state["market_open"]  = False
                state["ws_connected"] = False
                state["signal"]       = "MARKET CLOSED"
                state["reason"]       = "Mon–Fri 9:00 AM – 3:30 PM IST"
                state["last_update"]  = now.strftime("%H:%M:%S")
                add_log("Market closed — WebSocket will connect at 9:00 AM", "info")
                time.sleep(60)
                continue

            state["market_open"] = True
            add_log("Market open — connecting WebSocket...", "info")

            # reconnect Angel One session (token expires daily)
            obj = connect_angel()
            if obj is None:
                time.sleep(30)
                continue

            # start WebSocket
            ws = SmartWebSocketV2(
                _auth_token[0],
                API_KEY.strip(),
                CLIENT_CODE.strip(),
                _feed_token[0]
            )
            ws.on_open    = on_open
            ws.on_data    = on_data
            ws.on_error   = on_error
            ws.on_close   = on_close

            # subscribe to NIFTY 50 index
            token_list = [{"exchangeType": NIFTY_EXCH_TYPE, "tokens": [NIFTY_TOKEN]}]

            def run_ws():
                ws.connect()
                ws.subscribe("abc123", 3, token_list)  # mode 3 = full data with volume

            ws_thread = threading.Thread(target=run_ws, daemon=True)
            ws_thread.start()

            # keep alive until market closes
            while True:
                now = datetime.now(IST).replace(tzinfo=None)
                if now > now.replace(hour=15, minute=30, second=0):
                    ws.close_connection()
                    if state["position"]:
                        close_trade(obj, "Market Close")
                    break
                time.sleep(30)

        except Exception as e:
            add_log(f"Bot error: {e}", "error")
            time.sleep(30)

# ═══════════════════════════════════════
#   API
# ═══════════════════════════════════════
@app.route("/api/state")
def api_state():
    total = state["wins"] + state["losses"]
    pos   = state["position"]
    return jsonify({
        "connected"    : state["connected"],
        "ws_connected" : state["ws_connected"],
        "market_open"  : state["market_open"],
        "nifty_price"  : state["nifty_price"],
        "signal"       : state["signal"],
        "reason"       : state["reason"],
        "balance"      : round(state["balance"], 2),
        "total_pnl"    : round(state["total_pnl"], 2),
        "pnl_pct"      : round(state["total_pnl"] / CAPITAL * 100, 2),
        "wins"         : state["wins"],
        "losses"       : state["losses"],
        "winrate"      : round(state["wins"] / total * 100, 1) if total > 0 else 0,
        "position"     : pos,
        "trades"       : state["trades"][:20],
        "log"          : state["log"][:30],
        "last_update"  : state["last_update"],
        "ema9"         : state["ema9"],
        "ema21"        : state["ema21"],
        "vwap"         : state["vwap"],
        "adx"          : state["adx_value"],
        "support"      : state["support"],
        "resistance"   : state["resistance"],
        "ema_signal"   : state["ema_signal"],
        "vwap_signal"  : state["vwap_signal"],
        "sr_signal"    : state["sr_signal"],
        "buy_votes"    : state["buy_votes"],
        "sell_votes"   : state["sell_votes"],
        "in_session"   : state["in_session"],
        "tick_count"   : state["tick_count"],
        "micro_count"  : state["micro_count"],
        "capital"      : CAPITAL,
        "sl_price"     : round(pos["sl_price"],  2) if pos else 0,
        "tgt_price"    : round(pos["tgt_price"], 2) if pos else 0,
    })

# ═══════════════════════════════════════
#   DASHBOARD
# ═══════════════════════════════════════
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NSEAlgo v4 — WebSocket Bot</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Sora:wght@700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#07090f;color:#e2e8f0;font-family:'JetBrains Mono',monospace;min-height:100vh;font-size:13px}
.hdr{background:#0f1520;border-bottom:1px solid #2a3850;padding:11px 18px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:9}
.logo{font-family:'Sora',sans-serif;font-size:15px;font-weight:700;color:#fff}.logo em{color:#f97316;font-style:normal}
.live{width:7px;height:7px;border-radius:50%;background:#22c55e;display:inline-block;animation:blink 1.1s infinite;margin-right:5px}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.upd{font-size:10px;color:#94a3b8;padding:2px 9px;background:#0f151e;border-radius:99px;border:1px solid #2a3850}
.wrap{max-width:960px;margin:0 auto;padding:14px 14px 40px}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:11px;margin-bottom:11px}
.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:11px}
.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:11px}
.card{background:#0f1520;border:1px solid #2a3850;border-radius:10px;padding:13px 15px}
.ct{font-size:9px;color:#94a3b8;letter-spacing:.12em;text-transform:uppercase;margin-bottom:7px}
.big{font-family:'Sora',sans-serif;font-size:24px;font-weight:700;color:#fff;letter-spacing:-.02em}
.sm{font-size:12px;font-weight:600;color:#fff}
.g{color:#22c55e}.r{color:#f43f5e}.o{color:#f97316}.y{color:#fbbf24}.pu{color:#a78bfa}
.sig{display:inline-flex;align-items:center;gap:6px;padding:5px 13px;border-radius:6px;font-size:11px;font-weight:700;letter-spacing:.06em}
.sbuy{background:#22c55e20;color:#4ade80;border:1px solid #22c55e50}
.ssell{background:#f43f5e20;color:#fb7185;border:1px solid #f43f5e50}
.shold{background:#ffffff10;color:#94a3b8;border:1px solid #ffffff18}
.sclosed{background:#fbbf2420;color:#fcd34d;border:1px solid #fbbf2450}
.pcard{border:1px solid #f9731650;background:#f9731608}
.srow{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid #2a385040;font-size:11px}
.srow:last-child{border-bottom:none}
.vote{flex:1;padding:7px;border-radius:5px;text-align:center;font-size:11px;font-weight:700}
.vbuy{background:#22c55e15;color:#4ade80;border:1px solid #22c55e25}
.vsell{background:#f43f5e15;color:#fb7185;border:1px solid #f43f5e25}
.lr{padding:5px 8px;border-radius:4px;font-size:10px;margin-bottom:3px;border-left:2px solid #2a3850;line-height:1.4}
.lbuy{border-left-color:#22c55e;color:#4ade80}
.lsell{border-left-color:#f43f5e;color:#fb7185}
.lprofit{border-left-color:#22c55e80;color:#4ade80}
.lloss{border-left-color:#f43f5e80;color:#fb7185}
.lsuccess{border-left-color:#22c55e;color:#4ade80}
.lerror{border-left-color:#f43f5e;color:#fb7185}
.linfo{border-left-color:#2a3850;color:#94a3b8}
table{width:100%;border-collapse:collapse;font-size:11px}
th{color:#94a3b8;font-size:9px;letter-spacing:.1em;padding:5px 7px;text-align:left;border-bottom:1px solid #2a3850}
td{padding:6px 7px;border-bottom:1px solid #2a385025;color:#cbd5e1}
.pill{display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600}
.pill-g{background:#22c55e20;color:#4ade80;border:1px solid #22c55e30}
.pill-r{background:#f43f5e20;color:#fb7185;border:1px solid #f43f5e30}
.pill-y{background:#fbbf2420;color:#fcd34d;border:1px solid #fbbf2430}
.pill-b{background:#3b82f620;color:#93c5fd;border:1px solid #3b82f630}
@media(max-width:600px){.g4{grid-template-columns:1fr 1fr}.g3{grid-template-columns:1fr}.g2{grid-template-columns:1fr}.big{font-size:19px}}
</style>
</head>
<body>
<div class="hdr">
  <div class="logo"><em>NSE</em>Algo <span style="font-size:10px;font-weight:400;color:#64748b;margin-left:6px">v4 · WebSocket · NIFTY F&O</span></div>
  <div style="display:flex;align-items:center;gap:8px">
    <span id="li" style="font-size:10px;color:#64748b">Connecting...</span>
    <span id="ws" class="pill pill-r">WS Off</span>
    <span id="sess" class="pill pill-y" style="display:none">—</span>
    <span class="upd" id="upd">—</span>
  </div>
</div>

<div class="wrap">

  <!-- Price + Signal -->
  <div class="g2">
    <div class="card">
      <div class="ct">NIFTY 50 · Live WebSocket price</div>
      <div class="big" id="np">₹—</div>
      <div style="margin-top:8px;display:flex;gap:14px;font-size:11px;flex-wrap:wrap">
        <span>EMA9: <span id="e9" class="o">—</span></span>
        <span>EMA21: <span id="e21" class="pu">—</span></span>
        <span>VWAP: <span id="vwap" class="y">—</span></span>
        <span>ADX: <span id="adx" class="g">—</span></span>
      </div>
      <div style="margin-top:6px;display:flex;gap:10px;font-size:10px;color:#64748b">
        <span>Ticks: <span id="tc" class="g">0</span></span>
        <span>Micro candles: <span id="mc" class="y">0</span>/12</span>
      </div>
    </div>
    <div class="card">
      <div class="ct">Final signal (2/3 vote + ADX filter)</div>
      <div id="sb"><span class="sig shold">— WAITING</span></div>
      <div style="margin-top:8px;font-size:11px;color:#94a3b8" id="sr">—</div>
      <div style="display:flex;gap:8px;margin-top:10px">
        <div class="vote vbuy" id="bv">🟢 BUY 0/3</div>
        <div class="vote vsell" id="sv">🔴 SELL 0/3</div>
      </div>
    </div>
  </div>

  <!-- Stats -->
  <div class="g4">
    <div class="card">
      <div class="ct">Balance</div>
      <div class="big" style="font-size:18px" id="bal">₹—</div>
    </div>
    <div class="card">
      <div class="ct">Total P&L</div>
      <div class="big" style="font-size:18px" id="pnl">₹0</div>
      <div style="font-size:10px;margin-top:2px" id="pnlp">0.00%</div>
    </div>
    <div class="card">
      <div class="ct">Win rate</div>
      <div class="big y" style="font-size:18px" id="wr">0%</div>
      <div style="font-size:10px;color:#94a3b8;margin-top:2px" id="trd">0 trades</div>
    </div>
    <div class="card">
      <div class="ct">Wins / Losses</div>
      <div style="display:flex;gap:10px;align-items:baseline;margin-top:3px">
        <span class="big g" style="font-size:18px" id="wi">0</span>
        <span style="color:#475569">/</span>
        <span class="big r" style="font-size:18px" id="lo">0</span>
      </div>
    </div>
  </div>

  <!-- Strategy Breakdown -->
  <div class="card" style="margin-bottom:11px">
    <div class="ct">Strategy breakdown — 3 signals voting every 3 minutes</div>
    <div class="srow">
      <span class="o" style="font-size:11px;font-weight:600">EMA 9/21 (on 15-sec averages)</span>
      <span id="ema-s" style="font-size:11px;color:#94a3b8">—</span>
    </div>
    <div class="srow">
      <span class="y" style="font-size:11px;font-weight:600">VWAP (Volume Weighted Avg Price)</span>
      <span id="vwap-s" style="font-size:11px;color:#94a3b8">—</span>
    </div>
    <div class="srow">
      <span class="pu" style="font-size:11px;font-weight:600">Support / Resistance</span>
      <span id="sr-s" style="font-size:11px;color:#94a3b8">—</span>
    </div>
    <div style="display:flex;gap:16px;margin-top:10px;font-size:11px;flex-wrap:wrap">
      <span>Support: <span class="g" id="sup">—</span></span>
      <span>Resistance: <span class="r" id="res">—</span></span>
      <span>ADX: <span class="y" id="adx2">—</span> <span style="color:#64748b">(≥25 = trending)</span></span>
    </div>
  </div>

  <!-- Open Position -->
  <div class="card pcard" id="pc" style="margin-bottom:11px;display:none">
    <div class="ct">📂 Open position</div>
    <div id="pg" style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:4px"></div>
  </div>
  <div class="card" id="npc" style="margin-bottom:11px;text-align:center;padding:14px">
    <div style="font-size:11px;color:#64748b">📂 No open position — signal checked every 3 mins during session</div>
  </div>

  <!-- Trade history + Log -->
  <div class="g2">
    <div class="card">
      <div class="ct">Trade history</div>
      <div id="te" style="font-size:11px;color:#64748b;padding:8px 0">No trades yet today</div>
      <table id="tt" style="display:none">
        <thead><tr><th>Symbol</th><th>Lots</th><th>P&L</th><th>Reason</th><th>Time</th></tr></thead>
        <tbody id="tb"></tbody>
      </table>
    </div>
    <div class="card">
      <div class="ct">Activity log</div>
      <div id="lc" style="max-height:280px;overflow-y:auto"></div>
    </div>
  </div>

</div>

<script>
const fi = n => new Intl.NumberFormat('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2}).format(n);

async function tick(){
  try{
    const d = await fetch('/api/state').then(r=>r.json());

    document.getElementById('li').innerHTML = d.connected ? '<span class="live"></span>Live' : 'Connecting...';
    document.getElementById('upd').textContent = 'Updated ' + d.last_update;

    // WS badge
    const wsEl = document.getElementById('ws');
    if(d.ws_connected){ wsEl.className='pill pill-g'; wsEl.textContent='WS Live'; }
    else { wsEl.className='pill pill-r'; wsEl.textContent='WS Off'; }

    // session badge
    const sb2 = document.getElementById('sess');
    sb2.style.display='inline-block';
    if(d.in_session){ sb2.className='pill pill-g'; sb2.textContent='✓ Session'; }
    else if(d.market_open){ sb2.className='pill pill-y'; sb2.textContent='Outside Session'; }
    else { sb2.className='pill pill-r'; sb2.textContent='Market Closed'; }

    // price + indicators
    document.getElementById('np').textContent   = '₹' + fi(d.nifty_price);
    document.getElementById('e9').textContent   = d.ema9  || '—';
    document.getElementById('e21').textContent  = d.ema21 || '—';
    document.getElementById('vwap').textContent = d.vwap  ? '₹'+fi(d.vwap) : '—';
    document.getElementById('adx').textContent  = d.adx   ? d.adx.toFixed(1) : '—';
    document.getElementById('adx').style.color  = d.adx >= 15 ? '#4ade80' : '#fb7185';
    document.getElementById('tc').textContent   = d.tick_count  || 0;
    document.getElementById('mc').textContent   = d.micro_count || 0;

    // signal
    const s = d.signal;
    let sc='shold', ic='—';
    if(s==='BUY'){sc='sbuy';ic='▲'} else if(s==='SELL'){sc='ssell';ic='▼'} else if(s==='MARKET CLOSED'){sc='sclosed';ic='💤'}
    document.getElementById('sb').innerHTML = `<span class="sig ${sc}">${ic} ${s}</span>`;
    document.getElementById('sr').textContent = d.reason;
    document.getElementById('bv').textContent = '🟢 BUY ' + d.buy_votes + '/3';
    document.getElementById('sv').textContent = '🔴 SELL ' + d.sell_votes + '/3';

    // stats
    const pe = document.getElementById('pnl');
    pe.textContent = (d.total_pnl>=0?'+':'')+'₹'+fi(Math.abs(d.total_pnl));
    pe.className = 'big '+(d.total_pnl>=0?'g':'r');
    pe.style.fontSize='18px';
    document.getElementById('pnlp').textContent = (d.pnl_pct>=0?'+':'')+d.pnl_pct+'%';
    document.getElementById('pnlp').style.color = d.pnl_pct>=0?'#4ade80':'#fb7185';
    document.getElementById('bal').textContent  = '₹'+fi(d.balance);
    document.getElementById('wr').textContent   = d.winrate+'%';
    document.getElementById('trd').textContent  = (d.wins+d.losses)+' trades';
    document.getElementById('wi').textContent   = d.wins;
    document.getElementById('lo').textContent   = d.losses;

    // strategy
    document.getElementById('ema-s').textContent  = d.ema_signal  || '—';
    document.getElementById('vwap-s').textContent = d.vwap_signal || '—';
    document.getElementById('sr-s').textContent   = d.sr_signal   || '—';
    document.getElementById('sup').textContent    = d.support    ? '₹'+fi(d.support)    : '—';
    document.getElementById('res').textContent    = d.resistance ? '₹'+fi(d.resistance) : '—';
    document.getElementById('adx2').textContent   = d.adx ? d.adx.toFixed(1) : '—';
    document.getElementById('adx2').style.color   = d.adx >= 15 ? '#4ade80' : '#fb7185';

    // position
    if(d.position){
      document.getElementById('pc').style.display='block';
      document.getElementById('npc').style.display='none';
      const p=d.position;
      document.getElementById('pg').innerHTML=`
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">SYMBOL</div><div class="sm o">${p.symbol}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">ENTRY</div><div class="sm">₹${fi(p.entry_price)}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">LOTS × 65</div><div class="sm">${p.lots} lots = ${p.qty} units</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">STOP LOSS</div><div class="sm r">₹${fi(d.sl_price)}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">TARGET</div><div class="sm g">₹${fi(d.tgt_price)}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">ENTRY TIME</div><div class="sm">${p.entry_time}</div></div>`;
    } else {
      document.getElementById('pc').style.display='none';
      document.getElementById('npc').style.display='block';
    }

    // trades
    if(d.trades.length>0){
      document.getElementById('te').style.display='none';
      document.getElementById('tt').style.display='table';
      document.getElementById('tb').innerHTML=d.trades.map(t=>`
        <tr>
          <td class="o">${t.symbol}</td>
          <td>${t.lots||1}</td>
          <td class="${t.pnl>=0?'g':'r'}">${t.pnl>=0?'+':''}₹${fi(Math.abs(t.pnl))}</td>
          <td style="color:#94a3b8;font-size:10px">${t.reason}</td>
          <td style="color:#64748b">${t.entry_time}</td>
        </tr>`).join('');
    }

    // log
    document.getElementById('lc').innerHTML=d.log.map(l=>`
      <div class="lr l${l.level}">
        <span style="color:#475569">${l.time} </span>${l.msg}
      </div>`).join('');

  }catch(e){console.error(e)}
}

tick();
setInterval(tick, 3000);  // refresh every 3 seconds (faster for live data)
</script>
</body>
</html>"""

@app.route("/")
def home():
    return render_template_string(HTML)

threading.Thread(target=bot_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
