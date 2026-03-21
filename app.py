from flask import Flask, jsonify, render_template_string
from SmartApi import SmartConnect
import pyotp, pandas as pd, threading, time, os, requests, json
from datetime import datetime, timedelta
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
CAPITAL     = 1000000
LOT_SIZE    = 25
STOP_LOSS   = -1500   # tighter SL
TARGET      = 4500    # 1:3 risk reward
NIFTY_TOKEN = "99926000"
NIFTY_EXCH  = "NSE"

# Best trading windows (IST)
SESSION_1_START = (9, 30)
SESSION_1_END   = (11, 30)
SESSION_2_START = (13, 30)
SESSION_2_END   = (15, 0)

# ═══════════════════════════════════════
#   SHARED STATE
# ═══════════════════════════════════════
state = {
    "connected"    : False,
    "nifty_price"  : 0.0,
    "signal"       : "WAITING",
    "reason"       : "Starting up...",
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
    "rsi"          : 0.0,
    "support"      : 0.0,
    "resistance"   : 0.0,
    "sr_signal"    : "—",
    "ema_signal"   : "—",
    "vol_signal"   : "—",
    "buy_votes"    : 0,
    "sell_votes"   : 0,
    "in_session"   : False,
}

def add_log(msg, level="info"):
    t = datetime.now(IST).strftime("%H:%M:%S")
    entry = {"msg": msg, "level": level, "time": t}
    state["log"].insert(0, entry)
    state["log"] = state["log"][:60]
    print(f"[{t}] {msg}")

# ═══════════════════════════════════════
#   SYMBOL MASTER (for option tokens)
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
def connect_angel():
    for attempt in range(1, 6):
        try:
            obj  = SmartConnect(api_key=API_KEY.strip())
            totp = pyotp.TOTP(TOTP_TOKEN.strip()).now()
            data = obj.generateSession(CLIENT_CODE.strip(), PASSWORD.strip(), totp)
            if not data.get("status"):
                raise Exception(f"Login failed: {data.get('message', 'unknown')}")
            state["connected"] = True
            add_log("Connected to Angel One SmartAPI ✓", "success")
            return obj
        except Exception as e:
            add_log(f"Login attempt {attempt}/5 failed: {e}", "error")
            time.sleep(15)
    return None

# ═══════════════════════════════════════
#   GET NIFTY CANDLE DATA
# ═══════════════════════════════════════
def get_candle_data(obj):
    now = datetime.now(IST).replace(tzinfo=None)
    params = {
        "exchange"    : NIFTY_EXCH,
        "symboltoken" : NIFTY_TOKEN,
        "interval"    : "FIVE_MINUTE",
        "fromdate"    : (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M"),
        "todate"      : now.strftime("%Y-%m-%d %H:%M"),
    }
    data = obj.getCandleData(params)
    if not data.get("status"):
        raise Exception(f"Candle data error: {data.get('message')}")
    df = pd.DataFrame(data["data"],
        columns=["timestamp","open","high","low","close","volume"])
    for col in ["close","high","low","volume","open"]:
        df[col] = df[col].astype(float)
    return df

# ═══════════════════════════════════════
#   STRATEGY 1: EMA 9/21 CROSSOVER + RSI
# ═══════════════════════════════════════
def calc_ema_rsi(df):
    df["ema9"]  = df["close"].ewm(span=9,  adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi"] = 100 - (100 / (1 + gain / loss))

    last = df.iloc[-1]
    prev = df.iloc[-2]

    state["ema9"]  = round(float(last["ema9"]),  2)
    state["ema21"] = round(float(last["ema21"]), 2)
    rsi = round(float(last["rsi"]), 1)
    state["rsi"]   = rsi

    fresh_bull = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    fresh_bear = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]
    bull = last["ema9"] > last["ema21"] and 40 < rsi < 68
    bear = last["ema9"] < last["ema21"] and rsi > 32

    if fresh_bull and bull:
        state["ema_signal"] = f"BUY — EMA crossover ↑ RSI {rsi}"
        return "BUY"
    if fresh_bear and bear:
        state["ema_signal"] = f"SELL — EMA crossover ↓ RSI {rsi}"
        return "SELL"
    if bull:
        state["ema_signal"] = f"BUY — Bullish trend RSI {rsi}"
        return "BUY"
    if bear:
        state["ema_signal"] = f"SELL — Bearish trend RSI {rsi}"
        return "SELL"
    state["ema_signal"] = f"NEUTRAL — RSI {rsi}"
    return "NEUTRAL"

# ═══════════════════════════════════════
#   STRATEGY 2: SUPPORT & RESISTANCE
# ═══════════════════════════════════════
def calc_support_resistance(df):
    # Use last 20 candles for S&R levels
    recent     = df.tail(20)
    resistance = round(recent["high"].max(), 2)
    support    = round(recent["low"].min(),  2)
    price      = float(df["close"].iloc[-1])
    prev_price = float(df["close"].iloc[-2])
    buffer     = (resistance - support) * 0.015  # 1.5% buffer zone

    state["support"]    = support
    state["resistance"] = resistance

    # Price breaking above resistance = BUY
    if prev_price < resistance and price >= resistance - buffer:
        state["sr_signal"] = f"SELL — Near resistance ₹{resistance}"
        return "SELL"

    # Price bouncing from support = BUY
    if prev_price > support and price <= support + buffer:
        state["sr_signal"] = f"BUY — Near support ₹{support}"
        return "BUY"

    # Price in middle zone
    mid = (resistance + support) / 2
    if price > mid:
        state["sr_signal"] = f"BUY — Above midzone ₹{mid:.0f}"
        return "BUY"
    else:
        state["sr_signal"] = f"SELL — Below midzone ₹{mid:.0f}"
        return "SELL"

# ═══════════════════════════════════════
#   STRATEGY 3: VOLUME MOMENTUM
# ═══════════════════════════════════════
def calc_volume_momentum(df):
    df["vol_avg"] = df["volume"].rolling(10).mean()
    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    ratio = last["volume"] / last["vol_avg"] if last["vol_avg"] > 0 else 1
    high_vol = last["volume"] > last["vol_avg"] * 1.3

    if high_vol and last["close"] > prev["close"]:
        state["vol_signal"] = f"BUY — Volume surge ↑ {ratio:.1f}x"
        return "BUY"
    if high_vol and last["close"] < prev["close"]:
        state["vol_signal"] = f"SELL — Volume surge ↓ {ratio:.1f}x"
        return "SELL"

    state["vol_signal"] = f"NEUTRAL — Normal vol {ratio:.1f}x"
    return "NEUTRAL"

# ═══════════════════════════════════════
#   COMBINED SIGNAL (2 out of 3 vote)
# ═══════════════════════════════════════
def get_signal(df):
    if len(df) < 25:
        return "HOLD", "Not enough data"

    ema_s = calc_ema_rsi(df)
    sr_s  = calc_support_resistance(df)
    vol_s = calc_volume_momentum(df)

    buy_votes  = sum(1 for s in [ema_s, sr_s, vol_s] if s == "BUY")
    sell_votes = sum(1 for s in [ema_s, sr_s, vol_s] if s == "SELL")

    state["buy_votes"]  = buy_votes
    state["sell_votes"] = sell_votes

    if buy_votes >= 2:
        return "BUY",  f"{buy_votes}/3 strategies agree → BUY"
    if sell_votes >= 2:
        return "SELL", f"{sell_votes}/3 strategies agree → SELL"
    return "HOLD", "No consensus — waiting"

# ═══════════════════════════════════════
#   TIME SESSION CHECK
# ═══════════════════════════════════════
def is_good_session(now):
    h, m = now.hour, now.minute
    t = h * 60 + m
    s1_start = SESSION_1_START[0] * 60 + SESSION_1_START[1]
    s1_end   = SESSION_1_END[0]   * 60 + SESSION_1_END[1]
    s2_start = SESSION_2_START[0] * 60 + SESSION_2_START[1]
    s2_end   = SESSION_2_END[0]   * 60 + SESSION_2_END[1]
    in_s = (s1_start <= t <= s1_end) or (s2_start <= t <= s2_end)
    state["in_session"] = in_s
    return in_s

# ═══════════════════════════════════════
#   OPTION SYMBOL + LTP
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
        add_log(f"Token not found: {symbol}", "error")
        return None
    try:
        data    = obj.getMarketData("LTP", {"NFO": [token]})
        fetched = data.get("data", {}).get("fetched", [])
        if fetched:
            return float(fetched[0]["ltp"])
    except Exception as e:
        add_log(f"LTP error ({symbol}): {e}", "error")
    return None

# ═══════════════════════════════════════
#   OPEN TRADE
# ═══════════════════════════════════════
def open_trade(obj, signal, price, reason):
    symbol = get_option_symbol(price, signal)
    ltp    = get_option_ltp(obj, symbol)
    if ltp is None:
        add_log(f"Could not fetch LTP for {symbol}", "error")
        return
    cost = ltp * LOT_SIZE
    if cost > state["balance"]:
        add_log("Insufficient balance!", "error")
        return
    state["balance"]  -= cost
    state["position"]  = {
        "symbol"      : symbol,
        "type"        : "CE" if signal == "BUY" else "PE",
        "signal"      : signal,
        "entry_price" : ltp,
        "entry_nifty" : price,
        "cost"        : cost,
        "reason"      : reason,
        "entry_time"  : datetime.now(IST).strftime("%H:%M:%S"),
    }
    add_log(f"{'BUY' if signal=='BUY' else 'SELL'} {symbol} @ ₹{ltp:.2f} | Cost ₹{cost:,.0f}", "buy" if signal=="BUY" else "sell")

# ═══════════════════════════════════════
#   CLOSE TRADE
# ═══════════════════════════════════════
def close_trade(obj, reason="Signal"):
    pos = state["position"]
    ltp = get_option_ltp(obj, pos["symbol"])
    if ltp is None:
        add_log("Cannot close — LTP unavailable", "error")
        return
    pnl = (ltp - pos["entry_price"]) * LOT_SIZE
    state["balance"]   += ltp * LOT_SIZE
    state["total_pnl"] += pnl
    state["position"]   = None
    if pnl >= 0: state["wins"]   += 1
    else:        state["losses"] += 1
    state["trades"].insert(0, {
        "symbol"     : pos["symbol"],
        "entry"      : pos["entry_price"],
        "exit"       : ltp,
        "pnl"        : round(pnl, 2),
        "entry_time" : pos["entry_time"],
        "exit_time"  : datetime.now(IST).strftime("%H:%M:%S"),
        "reason"     : reason,
    })
    add_log(f"EXIT {pos['symbol']} @ ₹{ltp:.2f} | P&L {'+' if pnl>=0 else ''}₹{pnl:,.0f} ({reason})", "profit" if pnl>=0 else "loss")

# ═══════════════════════════════════════
#   MAIN BOT LOOP
# ═══════════════════════════════════════
def bot_loop():
    add_log("NSEAlgo v3 starting — S&R + EMA + Volume", "info")
    obj = None
    while obj is None:
        obj = connect_angel()
        if obj is None:
            time.sleep(30)
    load_symbol_master()
    add_log("Bot active — trading sessions: 9:30-11:30 & 13:30-15:00 IST", "info")

    while True:
        try:
            now          = datetime.now(IST).replace(tzinfo=None)
            market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
            market_close = now.replace(hour=15, minute=20, second=0, microsecond=0)
            is_weekend   = now.weekday() >= 5

            # ── outside market hours ──────────────────
            if is_weekend or not (market_open <= now <= market_close):
                if state["position"] and now > market_close and not is_weekend:
                    close_trade(obj, "Market Close")
                state["market_open"]  = False
                state["signal"]       = "MARKET CLOSED"
                state["reason"]       = "Mon–Fri 9:15 AM – 3:20 PM IST"
                state["last_update"]  = now.strftime("%H:%M:%S")
                time.sleep(60)
                continue

            state["market_open"] = True

            # ── fetch data ────────────────────────────
            df            = get_candle_data(obj)
            signal, reason = get_signal(df)
            price          = float(df["close"].iloc[-1])

            state["nifty_price"] = round(price, 2)
            state["signal"]      = signal
            state["reason"]      = reason
            state["last_update"] = now.strftime("%H:%M:%S")

            # ── check if in good trading session ──────
            good_session = is_good_session(now)
            pos          = state["position"]

            if pos is None:
                # only open trades during good sessions
                if signal in ("BUY", "SELL") and good_session:
                    open_trade(obj, signal, price, reason)
                elif signal in ("BUY", "SELL") and not good_session:
                    add_log(f"Signal {signal} ignored — outside trading session", "info")
            else:
                ltp = get_option_ltp(obj, pos["symbol"])
                if ltp:
                    unrealized = (ltp - pos["entry_price"]) * LOT_SIZE
                    if unrealized <= STOP_LOSS:
                        add_log(f"Stop loss hit! ₹{unrealized:,.0f}", "loss")
                        close_trade(obj, "Stop Loss")
                    elif unrealized >= TARGET:
                        add_log(f"Target hit! ₹{unrealized:,.0f}", "profit")
                        close_trade(obj, "Target")
                    elif (pos["type"] == "CE" and signal == "SELL") or \
                         (pos["type"] == "PE" and signal == "BUY"):
                        close_trade(obj, f"Signal flip → {signal}")
                    # force close before session ends
                    h, m = now.hour, now.minute
                    if (h == 11 and m >= 25) or (h == 14 and m >= 55):
                        close_trade(obj, "Session End")

        except Exception as e:
            add_log(f"Bot error: {e}", "error")
            try:
                obj = connect_angel() or obj
            except:
                pass

        sleep_secs = 300 - (time.time() % 300)
        time.sleep(max(5, sleep_secs))

# ═══════════════════════════════════════
#   API
# ═══════════════════════════════════════
@app.route("/api/state")
def api_state():
    total = state["wins"] + state["losses"]
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
        "position"    : state["position"],
        "trades"      : state["trades"][:20],
        "log"         : state["log"][:30],
        "last_update" : state["last_update"],
        "ema9"        : state["ema9"],
        "ema21"       : state["ema21"],
        "rsi"         : state["rsi"],
        "support"     : state["support"],
        "resistance"  : state["resistance"],
        "ema_signal"  : state["ema_signal"],
        "sr_signal"   : state["sr_signal"],
        "vol_signal"  : state["vol_signal"],
        "buy_votes"   : state["buy_votes"],
        "sell_votes"  : state["sell_votes"],
        "in_session"  : state["in_session"],
        "capital"     : CAPITAL,
        "sl"          : abs(STOP_LOSS),
        "target"      : TARGET,
    })

# ═══════════════════════════════════════
#   DASHBOARD
# ═══════════════════════════════════════
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NSEAlgo — NIFTY Paper Bot</title>
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
.g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:11px;margin-bottom:11px}
.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:11px}
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
.ebar{height:5px;background:#1e2d40;border-radius:99px;overflow:hidden;margin-top:6px}
.efill{height:100%;border-radius:99px;transition:width .5s}
.pill{display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600}
.pill-g{background:#22c55e20;color:#4ade80;border:1px solid #22c55e30}
.pill-r{background:#f43f5e20;color:#fb7185;border:1px solid #f43f5e30}
.pill-y{background:#fbbf2420;color:#fcd34d;border:1px solid #fbbf2430}
@media(max-width:600px){.g4{grid-template-columns:1fr 1fr}.g3{grid-template-columns:1fr}.g2{grid-template-columns:1fr}.big{font-size:19px}}
</style>
</head>
<body>
<div class="hdr">
  <div class="logo"><em>NSE</em>Algo <span style="font-size:10px;font-weight:400;color:#64748b;margin-left:8px">PAPER TRADING · NIFTY F&O · v3</span></div>
  <div style="display:flex;align-items:center;gap:10px">
    <span id="li" style="font-size:10px;color:#64748b">Connecting...</span>
    <span id="sess" class="pill pill-y" style="display:none">Outside Session</span>
    <span class="upd" id="upd">—</span>
  </div>
</div>

<div class="wrap">

  <!-- Price + Signal -->
  <div class="g2">
    <div class="card">
      <div class="ct">NIFTY 50 live price</div>
      <div class="big" id="np">₹—</div>
      <div style="margin-top:8px;display:flex;gap:16px;font-size:11px">
        <span>EMA9: <span id="e9" class="o">—</span></span>
        <span>EMA21: <span id="e21" class="pu">—</span></span>
        <span>RSI: <span id="rsi" class="y">—</span></span>
      </div>
    </div>
    <div class="card">
      <div class="ct">Final signal (2/3 vote)</div>
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
      <div style="font-size:10px;color:#94a3b8;margin-top:2px" id="tc">0 trades</div>
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
    <div class="ct">Strategy breakdown — 3 strategies voting</div>
    <div class="srow">
      <span class="o" style="font-size:11px;font-weight:600">EMA 9/21 + RSI</span>
      <span id="ema-s" style="font-size:11px;color:#94a3b8">—</span>
    </div>
    <div class="srow">
      <span class="pu" style="font-size:11px;font-weight:600">Support / Resistance</span>
      <span id="sr-s" style="font-size:11px;color:#94a3b8">—</span>
    </div>
    <div class="srow">
      <span class="g" style="font-size:11px;font-weight:600">Volume Momentum</span>
      <span id="vol-s" style="font-size:11px;color:#94a3b8">—</span>
    </div>
    <div style="display:flex;gap:16px;margin-top:10px;font-size:11px">
      <span>Support: <span class="g" id="sup">—</span></span>
      <span>Resistance: <span class="r" id="res">—</span></span>
      <span>SL: <span class="r">₹<span id="sl">—</span></span></span>
      <span>Target: <span class="g">₹<span id="tg">—</span></span></span>
    </div>
  </div>

  <!-- Open Position -->
  <div class="card pcard" id="pc" style="margin-bottom:11px;display:none">
    <div class="ct">📂 Open position</div>
    <div id="pg" style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:4px"></div>
  </div>
  <div class="card" id="npc" style="margin-bottom:11px;text-align:center;padding:14px">
    <div style="font-size:11px;color:#64748b">📂 No open position — waiting for signal in trading session</div>
  </div>

  <!-- Trade history + Log -->
  <div class="g2">
    <div class="card">
      <div class="ct">Trade history</div>
      <div id="te" style="font-size:11px;color:#64748b;padding:8px 0">No trades yet today</div>
      <table id="tt" style="display:none">
        <thead><tr><th>Symbol</th><th>P&L</th><th>Reason</th><th>Time</th></tr></thead>
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

    // session badge
    const sb = document.getElementById('sess');
    sb.style.display = 'inline-block';
    if(d.in_session){ sb.className='pill pill-g'; sb.textContent='✓ Trading Session'; }
    else if(d.market_open){ sb.className='pill pill-y'; sb.textContent='Outside Session'; }
    else { sb.className='pill pill-r'; sb.textContent='Market Closed'; }

    // price
    document.getElementById('np').textContent = '₹' + fi(d.nifty_price);
    document.getElementById('e9').textContent  = d.ema9  || '—';
    document.getElementById('e21').textContent = d.ema21 || '—';
    document.getElementById('rsi').textContent = d.rsi   || '—';

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
    pe.className = 'big ' + (d.total_pnl>=0?'g':'r');
    pe.style.fontSize = '18px';
    document.getElementById('pnlp').textContent = (d.pnl_pct>=0?'+':'')+d.pnl_pct+'%';
    document.getElementById('pnlp').style.color = d.pnl_pct>=0?'#4ade80':'#fb7185';
    document.getElementById('bal').textContent = '₹'+fi(d.balance);
    document.getElementById('wr').textContent  = d.winrate+'%';
    document.getElementById('tc').textContent  = (d.wins+d.losses)+' trades';
    document.getElementById('wi').textContent  = d.wins;
    document.getElementById('lo').textContent  = d.losses;

    // strategy breakdown
    document.getElementById('ema-s').textContent  = d.ema_signal  || '—';
    document.getElementById('sr-s').textContent   = d.sr_signal   || '—';
    document.getElementById('vol-s').textContent  = d.vol_signal  || '—';
    document.getElementById('sup').textContent    = d.support  ? '₹'+fi(d.support)     : '—';
    document.getElementById('res').textContent    = d.resistance ? '₹'+fi(d.resistance) : '—';
    document.getElementById('sl').textContent     = fi(d.sl);
    document.getElementById('tg').textContent     = fi(d.target);

    // position
    if(d.position){
      document.getElementById('pc').style.display='block';
      document.getElementById('npc').style.display='none';
      const p = d.position;
      document.getElementById('pg').innerHTML = `
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">SYMBOL</div><div class="sm o">${p.symbol}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">ENTRY PRICE</div><div class="sm">₹${fi(p.entry_price)}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">COST</div><div class="sm">₹${fi(p.cost)}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">NIFTY AT ENTRY</div><div class="sm">₹${fi(p.entry_nifty)}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">TYPE</div><div class="sm">${p.type}</div></div>
        <div><div style="font-size:9px;color:#64748b;margin-bottom:2px">ENTRY TIME</div><div class="sm">${p.entry_time}</div></div>`;
    } else {
      document.getElementById('pc').style.display='none';
      document.getElementById('npc').style.display='block';
    }

    // trades
    if(d.trades.length>0){
      document.getElementById('te').style.display='none';
      document.getElementById('tt').style.display='table';
      document.getElementById('tb').innerHTML = d.trades.map(t=>`
        <tr>
          <td class="o">${t.symbol}</td>
          <td class="${t.pnl>=0?'g':'r'}">${t.pnl>=0?'+':''}₹${fi(Math.abs(t.pnl))}</td>
          <td style="color:#94a3b8;font-size:10px">${t.reason}</td>
          <td style="color:#64748b">${t.entry_time}</td>
        </tr>`).join('');
    }

    // log
    document.getElementById('lc').innerHTML = d.log.map(l=>`
      <div class="lr l${l.level}">
        <span style="color:#475569">${l.time} </span>${l.msg}
      </div>`).join('');

  }catch(e){console.error(e)}
}

tick();
setInterval(tick, 10000);
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
