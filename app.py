from flask import Flask, jsonify, render_template_string
from SmartApi import SmartConnect
import pyotp, pandas as pd, threading, time, os
from datetime import datetime, timedelta
import pytz
IST = pytz.timezone("Asia/Kolkata")

app = Flask(__name__)

# ═══════════════════════════════════════
#   CREDENTIALS (set in Railway Variables)
# ═══════════════════════════════════════
API_KEY     = os.environ.get("API_KEY",     "")
CLIENT_CODE = os.environ.get("CLIENT_CODE", "")
PASSWORD    = os.environ.get("PASSWORD",    "")
TOTP_TOKEN  = os.environ.get("TOTP_TOKEN",  "")

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
#   SHARED STATE
# ═══════════════════════════════════════
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
    t = datetime.now(IST).strftime("%H:%M:%S")
    entry = {"msg": msg, "level": level, "time": t}
    state["log"].insert(0, entry)
    state["log"] = state["log"][:60]
    print(f"[{t}] {msg}")

# ═══════════════════════════════════════
#   CONNECT ANGEL ONE (with retry)
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
    df["close"]  = df["close"].astype(float)
    df["high"]   = df["high"].astype(float)
    df["low"]    = df["low"].astype(float)
    df["volume"] = df["volume"].astype(float)
    return df

# ═══════════════════════════════════════
#   STRATEGY: EMA CROSSOVER + RSI
# ═══════════════════════════════════════
def get_signal(df):
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

    fresh_bull = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    fresh_bear = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]
    bull_trend = last["ema9"] > last["ema21"] and 40 < rsi < 68
    bear_trend = last["ema9"] < last["ema21"] and rsi > 32

    if fresh_bull and bull_trend:
        return "BUY",  f"EMA crossover ↑ | RSI {rsi}"
    if fresh_bear and bear_trend:
        return "SELL", f"EMA crossover ↓ | RSI {rsi}"
    if bull_trend:
        return "BUY",  f"EMA bullish trend | RSI {rsi}"
    if bear_trend:
        return "SELL", f"EMA bearish trend | RSI {rsi}"
    return "HOLD", f"No clear signal | RSI {rsi}"


import requests,json

nfo_symbols = {}

def load_symbol_master():
    global nfo_symbols
    try:
        url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        data = requests.get(url, timeout=10).json()
        for item in data:
            if item.get("exch_seg") == "NFO" and "NIFTY" in item.get("symbol", ""):
                nfo_symbols[item["symbol"]] = item["token"]
        add_log(f"Loaded {len(nfo_symbols)} NIFTY NFO symbols", "info")
    except Exception as e:
        add_log(f"Symbol master load failed: {e}", "error")

def get_option_token(symbol):
    return nfo_symbols.get(symbol)


# ═══════════════════════════════════════
#   GET OPTION SYMBOL (NSE F&O)
# ═══════════════════════════════════════
def get_option_symbol(price, signal):
    strike = round(price / 50) * 50
    otype  = "CE" if signal == "BUY" else "PE"
    expiry = get_nearest_expiry()
    return f"NIFTY{expiry}{strike}{otype}"
def get_nearest_expiry():
    today = datetime.now(IST).replace(tzinfo=None)
    days_ahead = (3 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    expiry = today + timedelta(days=days_ahead)
    return expiry.strftime("%d%b%y").upper()
def get_option_ltp(obj, symbol):
    token = get_option_token(symbol)
    if not token:
        add_log(f"Token not found for {symbol}", "error")
        return None
    try:
        data = obj.getMarketData("LTP", {"NFO": [token]})
        fetched = data.get("data", {}).get("fetched", [])
        if fetched:
            return float(fetched[0]["ltp"])
    except Exception as e:
        add_log(f"LTP error ({symbol}): {e}", "error")
    return None


# ═══════════════════════════════════════
#   OPEN PAPER TRADE
# ═══════════════════════════════════════
def open_trade(obj, signal, price, reason):
    symbol = get_option_symbol(price, signal)
    ltp    = get_option_ltp(obj, symbol)

    if ltp is None:
        add_log(f"Could not fetch LTP for {symbol}", "error")
        return

    cost = ltp * LOT_SIZE
    if cost > state["balance"]:
        add_log("Insufficient balance to open trade", "error")
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
#   CLOSE PAPER TRADE
# ═══════════════════════════════════════
def close_trade(obj, reason="Signal"):
    pos = state["position"]
    ltp = get_option_ltp(obj, pos["symbol"])

    if ltp is None:
        add_log("Could not fetch LTP to close trade", "error")
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
    add_log("NSEAlgo bot starting...", "info")
    obj = None

    while obj is None:
        obj = connect_angel()
        load_symbol_master() 
        if obj is None:
            add_log("Retrying connection in 30s...", "error")
            time.sleep(30)

    add_log("Bot active — checking every 5 mins during market hours", "info")

    while True:
        try:
            now = datetime.now(IST).replace(tzinfo=None)
            market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
            market_close = now.replace(hour=15, minute=20, second=0, microsecond=0)
            is_weekend   = now.weekday() >= 5

            # ── outside market hours ──────────────────
            if is_weekend or not (market_open <= now <= market_close):
                # auto close any open position at day end
                if state["position"] and now > market_close and not is_weekend:
                    close_trade(obj, "Market Close")

                state["market_open"]  = False
                state["signal"]       = "MARKET CLOSED"
                state["reason"]       = "Mon–Fri 9:15 AM – 3:20 PM IST"
                state["last_update"]  = now.strftime("%H:%M:%S")
                time.sleep(60)
                continue

            state["market_open"] = True

            # ── fetch data & signal ───────────────────
            df     = get_candle_data(obj)
            signal, reason = get_signal(df)
            price  = float(df["close"].iloc[-1])

            state["nifty_price"] = round(price, 2)
            state["signal"]      = signal
            state["reason"]      = reason
            state["last_update"] = now.strftime("%H:%M:%S")

            pos = state["position"]

            # ── no position → open one ────────────────
            if pos is None:
                if signal in ("BUY", "SELL"):
                    open_trade(obj, signal, price, reason)

            # ── in position → check SL/target/flip ───
            else:
                ltp = get_option_ltp(obj, pos["symbol"])
                if ltp:
                    unrealized = (ltp - pos["entry_price"]) * LOT_SIZE

                    if unrealized <= STOP_LOSS:
                        add_log(f"Stop loss hit! Unrealized: ₹{unrealized:,.0f}", "loss")
                        close_trade(obj, "Stop Loss")
                    elif unrealized >= TARGET:
                        add_log(f"Target hit! Unrealized: ₹{unrealized:,.0f}", "profit")
                        close_trade(obj, "Target")
                    elif (pos["type"] == "CE" and signal == "SELL") or \
                         (pos["type"] == "PE" and signal == "BUY"):
                        close_trade(obj, f"Signal flip → {signal}")

        except Exception as e:
            add_log(f"Bot error: {e}", "error")
            try:
                obj = connect_angel() or obj
            except:
                pass

        # ── wait for next 5-min candle ────────────────
        sleep_secs = 300 - (time.time() % 300)
        time.sleep(sleep_secs)

# ═══════════════════════════════════════
#   API ENDPOINT
# ═══════════════════════════════════════
@app.route("/api/state")
def api_state():
    pos    = state["position"]
    unreal = 0.0
    total  = state["wins"] + state["losses"]
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

# ═══════════════════════════════════════
#   DASHBOARD HTML
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
.wrap{max-width:920px;margin:0 auto;padding:14px 14px 40px}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:11px;margin-bottom:11px}
.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:11px}
.card{background:#0f1520;border:1px solid #2a3850;border-radius:10px;padding:13px 15px}
.ct{font-size:9px;color:#94a3b8;letter-spacing:.12em;text-transform:uppercase;margin-bottom:7px}
.big{font-family:'Sora',sans-serif;font-size:24px;font-weight:700;color:#fff;letter-spacing:-.02em}
.sm{font-size:12px;font-weight:600;color:#fff}
.g{color:#22c55e}.r{color:#f43f5e}.o{color:#f97316}.y{color:#fbbf24}
.sig{display:inline-flex;align-items:center;gap:6px;padding:5px 13px;border-radius:6px;font-size:11px;font-weight:700;letter-spacing:.06em}
.sbuy{background:#22c55e20;color:#4ade80;border:1px solid #22c55e50}
.ssell{background:#f43f5e20;color:#fb7185;border:1px solid #f43f5e50}
.shold{background:#ffffff12;color:#94a3b8;border:1px solid #ffffff20}
.sclosed{background:#fbbf2420;color:#fcd34d;border:1px solid #fbbf2450}
.pcard{border:1px solid #f9731650;background:#f9731610}
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
td{padding:6px 7px;border-bottom:1px solid #2a385030;color:#cbd5e1}
.ebar{height:5px;background:#1e2d40;border-radius:99px;overflow:hidden;margin-top:5px}
.efill{height:100%;border-radius:99px;transition:width .5s}
@media(max-width:580px){.g4{grid-template-columns:1fr 1fr}.g2{grid-template-columns:1fr}.big{font-size:19px}}
</style>
</head>
<body>
<div class="hdr">
  <div class="logo"><em>NSE</em>Algo <span style="font-size:10px;font-weight:400;color:#2a3848;margin-left:8px">PAPER TRADING · NIFTY F&O</span></div>
  <div style="display:flex;align-items:center;gap:10px">
    <span id="li" style="font-size:10px;color:#2a3848">Connecting...</span>
    <span class="upd" id="upd">—</span>
  </div>
</div>

<div class="wrap">

  <!-- Price + Signal -->
  <div class="g2">
    <div class="card">
      <div class="ct">NIFTY 50 live price</div>
      <div class="big" id="np">₹—</div>
      <div style="margin-top:6px;font-size:10px;color:#2a3848">
        EMA9: <span id="e9" class="o">—</span> &nbsp;|&nbsp; EMA21: <span id="e21" style="color:#818cf8">—</span>
      </div>
    </div>
    <div class="card">
      <div class="ct">Current signal</div>
      <div id="sb"><span class="sig shold">— WAITING</span></div>
      <div style="margin-top:7px;font-size:10px;color:#445566" id="sr">—</div>
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
      <div style="font-size:10px;color:#2a3848;margin-top:2px" id="tc">0 trades</div>
    </div>
    <div class="card">
      <div class="ct">Wins / Losses</div>
      <div style="display:flex;gap:10px;align-items:baseline;margin-top:3px">
        <span class="big g" style="font-size:18px" id="wi">0</span>
        <span style="color:#2a3848">/</span>
        <span class="big r" style="font-size:18px" id="lo">0</span>
      </div>
    </div>
  </div>

  <!-- Open Position -->
  <div class="card pcard" id="pc" style="margin-bottom:11px;display:none">
    <div class="ct">📂 Open position</div>
    <div id="pg" style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:4px"></div>
    <div style="margin-top:10px;display:flex;gap:20px;font-size:10px">
      <span style="color:#2a3848">SL: <span class="r">-₹<span id="sl">—</span></span></span>
      <span style="color:#2a3848">Target: <span class="g">+₹<span id="tg">—</span></span></span>
      <span style="color:#2a3848">Unrealized: <span id="unr" class="sm">₹—</span></span>
    </div>
  </div>
  <div class="card" id="npc" style="margin-bottom:11px;text-align:center;padding:14px">
    <div style="font-size:10px;color:#2a3848">📂 No open position — waiting for signal</div>
  </div>

  <!-- EMA indicator -->
  <div class="card" style="margin-bottom:11px">
    <div class="ct">EMA 9 / 21 trend</div>
    <div style="display:flex;gap:24px;margin-top:4px">
      <div><div style="font-size:9px;color:#2a3848;margin-bottom:3px">EMA 9 (fast)</div><div class="sm o" id="ema9v">—</div></div>
      <div><div style="font-size:9px;color:#2a3848;margin-bottom:3px">EMA 21 (slow)</div><div class="sm" style="color:#818cf8" id="ema21v">—</div></div>
      <div><div style="font-size:9px;color:#2a3848;margin-bottom:3px">Spread</div><div class="sm" id="emaspread">—</div></div>
      <div><div style="font-size:9px;color:#2a3848;margin-bottom:3px">Trend</div><div class="sm" id="ematrend">—</div></div>
    </div>
    <div class="ebar" style="margin-top:10px">
      <div class="efill" id="emabar" style="width:50%;background:#22c55e"></div>
    </div>
  </div>

  <!-- Trade history + Log -->
  <div class="g2">
    <div class="card">
      <div class="ct">Trade history</div>
      <div id="te" style="font-size:10px;color:#2a3848;padding:8px 0">No trades yet today</div>
      <table id="tt" style="display:none">
        <thead><tr><th>Symbol</th><th>P&L</th><th>Reason</th><th>Time</th></tr></thead>
        <tbody id="tb"></tbody>
      </table>
    </div>
    <div class="card">
      <div class="ct">Activity log</div>
      <div id="lc" style="max-height:260px;overflow-y:auto"></div>
    </div>
  </div>

</div>

<script>
const fi = n => new Intl.NumberFormat('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2}).format(n);

async function tick(){
  try{
    const d = await fetch('/api/state').then(r=>r.json());

    // header
    document.getElementById('li').innerHTML = d.connected ? '<span class="live"></span>Live' : 'Connecting...';
    document.getElementById('upd').textContent = 'Updated ' + d.last_update;

    // price
    document.getElementById('np').textContent = '₹' + fi(d.nifty_price);
    document.getElementById('e9').textContent  = d.ema9  || '—';
    document.getElementById('e21').textContent = d.ema21 || '—';

    // signal
    const s = d.signal;
    let sc='shold', ic='—';
    if(s==='BUY'){sc='sbuy';ic='▲'} else if(s==='SELL'){sc='ssell';ic='▼'} else if(s==='MARKET CLOSED'){sc='sclosed';ic='💤'}
    document.getElementById('sb').innerHTML = `<span class="sig ${sc}">${ic} ${s}</span>`;
    document.getElementById('sr').textContent = d.reason;

    // stats
    const pe = document.getElementById('pnl');
    pe.textContent = (d.total_pnl>=0?'+':'')+'₹'+fi(Math.abs(d.total_pnl));
    pe.className = 'big ' + (d.total_pnl>=0?'g':'r') + ' ' ;
    pe.style.fontSize='18px';
    document.getElementById('pnlp').textContent = (d.pnl_pct>=0?'+':'')+d.pnl_pct+'%';
    document.getElementById('pnlp').style.color = d.pnl_pct>=0?'#22c55e':'#f43f5e';
    document.getElementById('bal').textContent = '₹'+fi(d.balance);
    document.getElementById('wr').textContent  = d.winrate+'%';
    document.getElementById('tc').textContent  = (d.wins+d.losses)+' trades';
    document.getElementById('wi').textContent  = d.wins;
    document.getElementById('lo').textContent  = d.losses;

    // position
    if(d.position){
      document.getElementById('pc').style.display='block';
      document.getElementById('npc').style.display='none';
      const p = d.position;
      document.getElementById('pg').innerHTML = `
        <div><div style="font-size:9px;color:#2a3848;margin-bottom:2px">SYMBOL</div><div class="sm o">${p.symbol}</div></div>
        <div><div style="font-size:9px;color:#2a3848;margin-bottom:2px">ENTRY</div><div class="sm">₹${fi(p.entry_price)}</div></div>
        <div><div style="font-size:9px;color:#2a3848;margin-bottom:2px">COST</div><div class="sm">₹${fi(p.cost)}</div></div>
        <div><div style="font-size:9px;color:#2a3848;margin-bottom:2px">NIFTY AT ENTRY</div><div class="sm">₹${fi(p.entry_nifty)}</div></div>
        <div><div style="font-size:9px;color:#2a3848;margin-bottom:2px">TYPE</div><div class="sm">${p.type}</div></div>
        <div><div style="font-size:9px;color:#2a3848;margin-bottom:2px">ENTRY TIME</div><div class="sm">${p.entry_time}</div></div>`;
      document.getElementById('sl').textContent = fi(d.sl);
      document.getElementById('tg').textContent = fi(d.target);
    } else {
      document.getElementById('pc').style.display='none';
      document.getElementById('npc').style.display='block';
    }

    // EMA
    document.getElementById('ema9v').textContent  = d.ema9  || '—';
    document.getElementById('ema21v').textContent = d.ema21 || '—';
    if(d.ema9 && d.ema21){
      const spread = (d.ema9 - d.ema21).toFixed(2);
      const bull = d.ema9 > d.ema21;
      document.getElementById('emaspread').textContent = (bull?'+':'')+spread;
      document.getElementById('emaspread').className = 'sm ' + (bull?'g':'r');
      document.getElementById('ematrend').textContent = bull ? '▲ Bullish' : '▼ Bearish';
      document.getElementById('ematrend').className = 'sm ' + (bull?'g':'r');
      const pct = Math.min(100, Math.max(0, 50 + (d.ema9-d.ema21)/d.ema21*2000));
      document.getElementById('emabar').style.width = pct+'%';
      document.getElementById('emabar').style.background = bull ? '#22c55e' : '#f43f5e';
    }

    // trades
    if(d.trades.length>0){
      document.getElementById('te').style.display='none';
      document.getElementById('tt').style.display='table';
      document.getElementById('tb').innerHTML = d.trades.map(t=>`
        <tr>
          <td class="o">${t.symbol}</td>
          <td class="${t.pnl>=0?'g':'r'}">${t.pnl>=0?'+':''}₹${fi(Math.abs(t.pnl))}</td>
          <td style="color:#2a3848;font-size:10px">${t.reason}</td>
          <td style="color:#2a3848">${t.entry_time}</td>
        </tr>`).join('');
    }

    // log
    document.getElementById('lc').innerHTML = d.log.map(l=>`
      <div class="lr l${l.level}">
        <span style="color:#2a3848">${l.time} </span>${l.msg}
      </div>`).join('');

  } catch(e){ console.error(e) }
}

tick();
setInterval(tick, 10000);
</script>
</body>
</html>"""

@app.route("/")
def home():
    return render_template_string(HTML)

# ═══════════════════════════════════════
#   START
# ═══════════════════════════════════════
threading.Thread(target=bot_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
