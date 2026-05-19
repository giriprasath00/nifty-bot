from flask import Flask, jsonify, render_template_string, request
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp, pandas as pd, threading, time, os, requests, json
from datetime import datetime, timedelta, date
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
CAPITAL          = 2000000      # 20 Lakhs
LOT_SIZE         = 75           # Updated Nifty lot size (check with broker)
CAPITAL_USE_PCT  = 0.10         # FIX: was 0.80 — risking 80% per trade is reckless; use 10%
SL_PCT           = 0.30         # FIX: 30% of premium (was fixed ₹10 — too tight)
TARGET_PCT       = 0.55         # FIX: 55% of premium (was fixed ₹25)
TRAIL_ACTIVATE   = 0.25         # Start trailing after +25% premium gain
TRAIL_LOCK_PCT   = 0.10         # Lock in at least 10% profit once trailing starts
MAX_DAILY_LOSS_PCT = 0.02       # 2% of capital = ₹40,000 max daily loss
MAX_TRADES_PER_DAY = 5          # Hard cap — quality over quantity
MAX_CONSEC_LOSSES  = 3          # Pause after 3 consecutive losses
CPR_BUFFER       = 15           # Points above/below CPR before it votes
ZONE_TOLERANCE   = 12.0         # S&R zone width (points)
MIN_CANDLES_SR   = 12           # Min 5-min candles before S&R votes (1 hour)
MIN_CANDLES_EMA  = 21           # Min candles for EMA21 to be valid
ADX_THRESHOLD    = 22           # Trend strength minimum (was 25 — slightly relaxed)
MAX_VIX          = 18.0         # Skip buying if VIX above this
MIN_PREMIUM      = 40           # Minimum option premium to trade (avoid illiquid)
SIGNAL_INTERVAL  = 180          # Seconds between signal computations
MICRO_INTERVAL   = 15           # Seconds between micro-buffer updates

NIFTY_TOKEN      = "99926000"
VIX_TOKEN        = "26000"
NIFTY_EXCH       = "NSE"
NIFTY_EXCH_TYPE  = 1

# Trading sessions IST
SESSION_1_START = (9,  30)
SESSION_1_END   = (11, 30)
SESSION_2_START = (13, 30)
SESSION_2_END   = (15,  0)

# ═══════════════════════════════════════
#   BUFFERS
# ═══════════════════════════════════════
tick_buffer       = deque(maxlen=50)
micro_buffer      = deque(maxlen=200)
candle_5m_buffer  = deque(maxlen=120)
current_5m_candle = None

last_micro_time   = [time.time()]
last_signal_time  = [time.time()]
last_reset_date   = [None]

# ═══════════════════════════════════════
#   SHARED STATE
# ═══════════════════════════════════════
state = {
    "connected"        : False,
    "ws_connected"     : False,
    "nifty_price"      : 0.0,
    "signal"           : "WAITING",
    "reason"           : "Connecting...",
    "balance"          : float(CAPITAL),
    "total_pnl"        : 0.0,
    "daily_pnl"        : 0.0,
    "daily_loss"       : 0.0,
    "wins"             : 0,
    "losses"           : 0,
    "consec_losses"    : 0,
    "trades_today"     : 0,
    "position"         : None,
    "trades"           : [],
    "last_update"      : "—",
    "market_open"      : False,
    "log"              : [],
    "ema9"             : 0.0,
    "ema21"            : 0.0,
    "adx"              : 0.0,
    "vix"              : 0.0,
    "support"          : 0.0,
    "resistance"       : 0.0,
    "ema_signal"       : "—",
    "vwap_signal"      : "—",
    "sr_signal"        : "—",
    "cpr_signal"       : "—",
    "adx_value"        : 0.0,
    "buy_votes"        : 0,
    "sell_votes"       : 0,
    "in_session"       : False,
    "tick_count"       : 0,
    "micro_count"      : 0,
    "candle_count"     : 0,
    "current_ltp"      : 0.0,
    "unrealized_pnl"   : 0.0,
    "pdh"              : 0.0,
    "pdl"              : 0.0,
    "cpr_p"            : 0.0,
    "cpr_tc"           : 0.0,
    "cpr_bc"           : 0.0,
    "r1":0.0,"r2":0.0,"r3":0.0,"r4":0.0,
    "s1":0.0,"s2":0.0,"s3":0.0,"s4":0.0,
    "swing_high"       : 0.0,
    "swing_low"        : 0.0,
    "bot_paused"       : False,
    "trade_source"     : "auto",
    "last_tick_time"   : 0.0,
    "flip_count"       : 0,          # FIX: consecutive flip counter
    "daily_reset_done" : False,
    "orb_high"         : 0.0,        # NEW: Opening Range Breakout levels
    "orb_low"          : 0.0,
    "orb_set"          : False,
    "orb_signal"       : "—",
    "market_bias"      : "NEUTRAL",  # NEW: overall bias from structure
    "signal_history"   : [],         # NEW: last 5 signals for confirmation
    "pause_reason"     : "",
}

def add_log(msg, level="info"):
    t = datetime.now(IST).strftime("%H:%M:%S")
    state["log"].insert(0, {"msg": msg, "level": level, "time": t})
    state["log"] = state["log"][:80]
    print(f"[{t}][{level.upper()}] {msg}")

# ═══════════════════════════════════════
#   PAUSE / RESUME HELPERS
# ═══════════════════════════════════════
def is_bot_paused():
    """All pause conditions in one place."""
    now = datetime.now(IST)

    # Tuesday Nifty expiry — pause after 1:30 PM
    if now.weekday() == 1 and now.strftime("%H:%M") >= "13:30":
        state["pause_reason"] = "Tuesday Nifty expiry — paused after 13:30"
        return True

    # Daily loss limit
    max_loss = CAPITAL * MAX_DAILY_LOSS_PCT
    if state["daily_loss"] >= max_loss:
        state["pause_reason"] = f"Daily loss limit ₹{max_loss:,.0f} hit — bot stopped for today"
        return True

    # Max trades per day
    if state["trades_today"] >= MAX_TRADES_PER_DAY:
        state["pause_reason"] = f"Max {MAX_TRADES_PER_DAY} trades reached for today"
        return True

    # Consecutive losses
    if state["consec_losses"] >= MAX_CONSEC_LOSSES:
        state["pause_reason"] = f"{MAX_CONSEC_LOSSES} consecutive losses — cooling down 30 min"
        return True

    # Manual pause
    if state.get("bot_paused"):
        state["pause_reason"] = "Manual pause active"
        return True

    return False

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
        expiry = get_nearest_expiry()
        samples = [s for s in nfo_symbols if expiry in s][:6]
        add_log(f"Target expiry: {expiry} | Samples: {samples}", "info")
    except Exception as e:
        add_log(f"Symbol master failed: {e}", "error")

def get_option_token(symbol):
    return nfo_symbols.get(symbol)

# ═══════════════════════════════════════
#   CONNECT ANGEL ONE
# ═══════════════════════════════════════
_obj        = [None]
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
#   DAILY LEVELS (CPR + Pivot)
# ═══════════════════════════════════════
def fetch_daily_levels(obj):
    try:
        to_date   = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        from_date = (datetime.now(IST) - timedelta(days=7)).strftime("%Y-%m-%d 09:15")
        params = {
            "exchange": NIFTY_EXCH, "symboltoken": NIFTY_TOKEN,
            "interval": "ONE_DAY", "fromdate": from_date, "todate": to_date
        }
        res = obj.getCandleData(params)
        if res and res.get("status") and res.get("data"):
            data = res["data"]
            yest = data[-2] if len(data) >= 2 else data[-1]
            state["pdh"]    = float(yest[2])
            state["pdl"]    = float(yest[3])
            pdc             = float(yest[4])
            state["cpr_p"]  = round((state["pdh"] + state["pdl"] + pdc) / 3, 2)
            state["cpr_bc"] = round((state["pdh"] + state["pdl"]) / 2, 2)
            state["cpr_tc"] = round((state["cpr_p"] - state["cpr_bc"]) + state["cpr_p"], 2)
            if state["cpr_bc"] > state["cpr_tc"]:
                state["cpr_tc"], state["cpr_bc"] = state["cpr_bc"], state["cpr_tc"]
            h, l, p = state["pdh"], state["pdl"], state["cpr_p"]
            rng = h - l
            state["r1"] = round((2*p)-l, 2);  state["s1"] = round((2*p)-h, 2)
            state["r2"] = round(p+rng, 2);    state["s2"] = round(p-rng, 2)
            state["r3"] = round(h+2*(p-l),2); state["s3"] = round(l-2*(h-p),2)
            state["r4"] = round(state["r3"]+rng,2); state["s4"] = round(state["s3"]-rng,2)
            add_log(f"Daily levels locked: PDH={h} PDL={l} CPR P={state['cpr_p']} TC={state['cpr_tc']} BC={state['cpr_bc']}", "success")
    except Exception as e:
        add_log(f"Failed to fetch daily levels: {e}", "error")

# ═══════════════════════════════════════
#   VIX FETCH
# ═══════════════════════════════════════
def fetch_vix(obj):
    """Fetch India VIX. Returns float or None."""
    try:
        data    = obj.getMarketData("LTP", {"NSE": [VIX_TOKEN]})
        fetched = data.get("data", {}).get("fetched", [])
        if fetched:
            vix = float(fetched[0]["ltp"])
            state["vix"] = round(vix, 2)
            return vix
    except Exception as e:
        add_log(f"VIX fetch error: {e}", "error")
    return None

# ═══════════════════════════════════════
#   WEBSOCKET HANDLERS
# ═══════════════════════════════════════
def parse_ltp(raw):
    if raw is None:
        return 0.0
    raw = float(raw)
    return raw / 100.0 if raw > 1_000_000 else raw

def on_data(wsapp, message):
    try:
        if not isinstance(message, dict):
            return
        state["last_tick_time"] = time.time()
        raw = (message.get("last_traded_price") or
               message.get("ltp") or message.get("close_price") or 0)
        ltp    = parse_ltp(raw)
        volume = (message.get("volume_trade_for_the_day") or
                  message.get("vol_traded_today") or message.get("volume") or 0)
        now    = datetime.now(IST).replace(tzinfo=None)
        mo_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        mo_end   = now.replace(hour=15, minute=20, second=0, microsecond=0)
        state["market_open"] = (mo_start <= now <= mo_end) and now.weekday() < 5
        state["last_update"] = now.strftime("%H:%M:%S")

        if ltp <= 100:
            return

        tick = {"price": ltp, "volume": volume, "time": now}
        tick_buffer.append(tick)

        # ── Build 5-minute candles ──────────────────────────
        global current_5m_candle
        minute_block    = now.minute - (now.minute % 5)
        candle_time_str = now.replace(minute=minute_block, second=0, microsecond=0).strftime("%H:%M")
        if current_5m_candle is None or current_5m_candle["time"] != candle_time_str:
            if current_5m_candle is not None:
                candle_5m_buffer.append(current_5m_candle)
                state["candle_count"] = len(candle_5m_buffer)
            current_5m_candle = {
                "time": candle_time_str,
                "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                "volume": volume
            }
        else:
            current_5m_candle["high"]   = max(current_5m_candle["high"], ltp)
            current_5m_candle["low"]    = min(current_5m_candle["low"],  ltp)
            current_5m_candle["close"]  = ltp
            current_5m_candle["volume"] = volume

        # ── ORB: set Opening Range after first 15-min candle ─
        update_orb(now, ltp)

        state["nifty_price"] = round(ltp, 2)
        state["tick_count"] += 1

        if time.time() - last_micro_time[0] >= MICRO_INTERVAL:
            compute_micro_average()
            last_micro_time[0] = time.time()

        if time.time() - last_signal_time[0] >= SIGNAL_INTERVAL:
            compute_signal_and_trade()
            last_signal_time[0] = time.time()

    except Exception as e:
        add_log(f"on_data error: {e}", "error")

_ws_ref        = [None]
_ws_token_list = [None]

def on_open(wsapp):
    state["ws_connected"] = True
    add_log("WebSocket connected — subscribing...", "success")
    def do_subscribe():
        time.sleep(0.5)
        try:
            _ws_ref[0].subscribe("abc123", 3, _ws_token_list[0])
            add_log("Subscribed to NIFTY 50 feed ✓", "success")
        except Exception as e:
            add_log(f"Subscribe error: {e}", "error")
    threading.Thread(target=do_subscribe, daemon=True).start()

def on_error(wsapp, error):
    state["ws_connected"] = False
    add_log(f"WebSocket error: {error}", "error")

def on_close(wsapp):
    state["ws_connected"] = False
    add_log("WebSocket disconnected", "error")

# ═══════════════════════════════════════
#   OPENING RANGE BREAKOUT (ORB)
# ═══════════════════════════════════════
def update_orb(now, ltp):
    """Sets ORB high/low from the first 15-minute candle (9:15–9:30)."""
    if state["orb_set"]:
        return
    orb_end = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now >= orb_end and len(candle_5m_buffer) >= 3:
        # First 3 × 5-min candles = first 15 minutes
        orb_candles      = list(candle_5m_buffer)[:3]
        state["orb_high"] = max(c["high"] for c in orb_candles)
        state["orb_low"]  = min(c["low"]  for c in orb_candles)
        state["orb_set"]  = True
        add_log(f"ORB set: High={state['orb_high']} Low={state['orb_low']}", "success")

def calc_orb_signal():
    if not state["orb_set"]:
        state["orb_signal"] = "NEUTRAL — ORB building"
        return "NEUTRAL"
    price = state["nifty_price"]
    buf   = ZONE_TOLERANCE
    if price > state["orb_high"] + buf:
        state["orb_signal"] = f"BUY — Breakout above ORB High {state['orb_high']}"
        return "BUY"
    elif price < state["orb_low"] - buf:
        state["orb_signal"] = f"SELL — Breakdown below ORB Low {state['orb_low']}"
        return "SELL"
    state["orb_signal"] = "NEUTRAL — Inside ORB"
    return "NEUTRAL"

# ═══════════════════════════════════════
#   MICRO AVERAGE (every 15 sec)
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

# ═══════════════════════════════════════
#   FIX: EMA on 5-MIN CANDLES (not micro-buffer)
# ═══════════════════════════════════════
def calc_ema_candles():
    """Calculate EMA9/21 on completed 5-min candle closes."""
    candles = list(candle_5m_buffer)
    if current_5m_candle:
        candles.append(current_5m_candle)  # include forming candle

    if len(candles) < MIN_CANDLES_EMA:
        state["ema_signal"] = f"NEUTRAL — Need {MIN_CANDLES_EMA} candles, have {len(candles)}"
        return 0.0, 0.0, "NEUTRAL"

    closes = pd.Series([c["close"] for c in candles])
    ema9   = float(closes.ewm(span=9,  adjust=False).mean().iloc[-1])
    ema21  = float(closes.ewm(span=21, adjust=False).mean().iloc[-1])
    state["ema9"]  = round(ema9, 2)
    state["ema21"] = round(ema21, 2)

    # Require meaningful separation (not just 1 point)
    diff = ema9 - ema21
    if abs(diff) < 3:
        state["ema_signal"] = f"NEUTRAL — EMA diff too small ({diff:.1f} pts)"
        return ema9, ema21, "NEUTRAL"

    if ema9 > ema21:
        state["ema_signal"] = f"BUY — EMA9 {ema9:.0f} > EMA21 {ema21:.0f} (Δ{diff:.1f})"
        return ema9, ema21, "BUY"
    else:
        state["ema_signal"] = f"SELL — EMA9 {ema9:.0f} < EMA21 {ema21:.0f} (Δ{diff:.1f})"
        return ema9, ema21, "SELL"

# ═══════════════════════════════════════
#   FIX: ADX on 5-MIN CANDLES
# ═══════════════════════════════════════
def calc_adx_candles(period=14):
    """ADX calculated on completed 5-min candles for reliable trend strength."""
    candles = list(candle_5m_buffer)
    if len(candles) < period + 1:
        return 0.0

    highs  = np.array([c["high"]  for c in candles])
    lows   = np.array([c["low"]   for c in candles])
    closes = np.array([c["close"] for c in candles])

    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, len(closes)):
        tr  = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        pdm = max(highs[i]-highs[i-1], 0) if (highs[i]-highs[i-1]) > (lows[i-1]-lows[i]) else 0
        ndm = max(lows[i-1]-lows[i], 0)   if (lows[i-1]-lows[i]) > (highs[i]-highs[i-1]) else 0
        tr_list.append(tr); pdm_list.append(pdm); ndm_list.append(ndm)

    if len(tr_list) < period:
        return 0.0

    atr = np.mean(tr_list[-period:])
    if atr == 0:
        return 0.0
    pdi = 100 * np.mean(pdm_list[-period:]) / atr
    ndi = 100 * np.mean(ndm_list[-period:]) / atr
    dx  = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) > 0 else 0
    return round(float(dx), 1)

# ═══════════════════════════════════════
#   RSI on 5-MIN CANDLES (new indicator)
# ═══════════════════════════════════════
def calc_rsi_candles(period=14):
    candles = list(candle_5m_buffer)
    if len(candles) < period + 1:
        return 50.0  # neutral default
    closes = pd.Series([c["close"] for c in candles])
    delta  = closes.diff()
    gain   = delta.clip(lower=0).rolling(period).mean()
    loss   = (-delta.clip(upper=0)).rolling(period).mean()
    rs     = gain / loss.replace(0, np.nan)
    rsi    = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 1)

# ═══════════════════════════════════════
#   SUPERTREND on 5-MIN CANDLES (new indicator)
# ═══════════════════════════════════════
def calc_supertrend(period=10, multiplier=3.0):
    candles = list(candle_5m_buffer)
    if len(candles) < period + 2:
        return "NEUTRAL"
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]

    atr_vals = []
    for i in range(1, len(closes)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        atr_vals.append(tr)
    if len(atr_vals) < period:
        return "NEUTRAL"
    atr = np.mean(atr_vals[-period:])
    hl2 = (highs[-1] + lows[-1]) / 2
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr
    price = closes[-1]
    if price > lower:
        state["vwap_signal"] = f"BUY — Supertrend bullish (lower={lower:.0f})"
        return "BUY"
    elif price < upper:
        state["vwap_signal"] = f"SELL — Supertrend bearish (upper={upper:.0f})"
        return "SELL"
    state["vwap_signal"] = "NEUTRAL"
    return "NEUTRAL"

# ═══════════════════════════════════════
#   CPR + POWER S&R (with buffer fix)
# ═══════════════════════════════════════
def find_strong_zones(price_list, min_touches=3):
    strong_zones = []
    used = set()
    for i, p1 in enumerate(price_list):
        if i in used:
            continue
        cluster = [p1]
        cluster_indices = {i}
        for j, p2 in enumerate(price_list):
            if i != j and j not in used and abs(p1 - p2) <= ZONE_TOLERANCE:
                cluster.append(p2)
                cluster_indices.add(j)
        if len(cluster) >= min_touches:
            strong_zones.append(sum(cluster) / len(cluster))
            used.update(cluster_indices)
    return strong_zones

def calc_cpr_sr_signal():
    price = state["nifty_price"]

    # FIX: CPR needs buffer — don't vote on marginal CPR breaks
    if price > state["cpr_tc"] + CPR_BUFFER:
        state["cpr_signal"] = f"BUY — Above CPR+{CPR_BUFFER} ({state['cpr_tc']:.0f})"
        cpr_vote = "BUY"
    elif price < state["cpr_bc"] - CPR_BUFFER:
        state["cpr_signal"] = f"SELL — Below CPR-{CPR_BUFFER} ({state['cpr_bc']:.0f})"
        cpr_vote = "SELL"
    else:
        state["cpr_signal"] = "NEUTRAL — Inside/Near CPR"
        cpr_vote = "NEUTRAL"

    # FIX: require minimum candles before S&R votes
    candles = list(candle_5m_buffer)
    if len(candles) < MIN_CANDLES_SR:
        state["sr_signal"] = f"NEUTRAL — Building S&R ({len(candles)}/{MIN_CANDLES_SR} candles)"
        return cpr_vote, "NEUTRAL"

    highs = [c["high"] for c in candles]
    lows  = [c["low"]  for c in candles]
    strong_res = find_strong_zones(highs, min_touches=3)
    strong_sup = find_strong_zones(lows,  min_touches=3)

    res_above = [z for z in strong_res if z >= price]
    sup_below = [z for z in strong_sup if z <= price]
    state["resistance"] = round(min(res_above), 2) if res_above else state["pdh"]
    state["support"]    = round(max(sup_below), 2) if sup_below else state["pdl"]

    sr_vote = "NEUTRAL"
    reason  = "No strong zone interaction"

    def in_zone(p, lvl):
        return lvl > 0 and (lvl - ZONE_TOLERANCE) <= p <= (lvl + ZONE_TOLERANCE)

    if in_zone(price, state["pdh"]) or any(in_zone(price, r) for r in strong_res):
        sr_vote, reason = "SELL", "In resistance zone (3-touch verified)"
    elif in_zone(price, state["pdl"]) or any(in_zone(price, s) for s in strong_sup):
        sr_vote, reason = "BUY", "In support zone (3-touch verified)"
    elif price > state["pdh"] + ZONE_TOLERANCE:
        sr_vote, reason = "BUY", "Breakout above PDH"
    elif price < state["pdl"] - ZONE_TOLERANCE:
        sr_vote, reason = "SELL", "Breakdown below PDL"

    # Pivot level bounces (R1, S1, R2, S2)
    for lvl, label, vote in [
        (state["r1"],"R1","SELL"),(state["s1"],"S1","BUY"),
        (state["r2"],"R2","SELL"),(state["s2"],"S2","BUY"),
    ]:
        if in_zone(price, lvl):
            sr_vote, reason = vote, f"At pivot {label} ({lvl:.0f})"
            break

    state["sr_signal"] = f"{sr_vote} — {reason}"
    return cpr_vote, sr_vote

# ═══════════════════════════════════════
#   SIGNAL HISTORY CONFIRMATION (new)
# ═══════════════════════════════════════
def update_signal_history(signal):
    state["signal_history"].append(signal)
    if len(state["signal_history"]) > 5:
        state["signal_history"].pop(0)

def get_confirmed_signal():
    """Returns signal only if last 2 of 3 historical signals agree."""
    hist = state["signal_history"]
    if len(hist) < 2:
        return "HOLD"
    recent = hist[-3:] if len(hist) >= 3 else hist
    buys   = recent.count("BUY")
    sells  = recent.count("SELL")
    if buys >= 2:
        return "BUY"
    if sells >= 2:
        return "SELL"
    return "HOLD"

# ═══════════════════════════════════════
#   MARKET BIAS (new — broad context)
# ═══════════════════════════════════════
def update_market_bias():
    """Determines overall market bias from price vs CPR and PDH/PDL."""
    price = state["nifty_price"]
    if not price:
        return "NEUTRAL"
    bullish_pts = 0
    bearish_pts = 0
    if price > state["cpr_tc"]: bullish_pts += 1
    if price < state["cpr_bc"]: bearish_pts += 1
    if price > state["pdh"]: bullish_pts += 1
    if price < state["pdl"]: bearish_pts += 1
    if state["ema9"] > state["ema21"] and state["ema9"] > 0: bullish_pts += 1
    if state["ema9"] < state["ema21"] and state["ema9"] > 0: bearish_pts += 1

    if bullish_pts >= 2:
        state["market_bias"] = "BULLISH"
    elif bearish_pts >= 2:
        state["market_bias"] = "BEARISH"
    else:
        state["market_bias"] = "NEUTRAL"

# ═══════════════════════════════════════
#   CORE SIGNAL ENGINE (rewritten)
# ═══════════════════════════════════════
def compute_signal_and_trade():
    try:
        # ── Gate 1: Pause checks ────────────────────────────
        if is_bot_paused():
            state["signal"] = "AUTO-PAUSED"
            state["reason"] = state["pause_reason"]
            return

        # ── Gate 2: Market hours ────────────────────────────
        if not is_market_hours():
            state["signal"] = "MARKET CLOSED"
            state["reason"] = "Mon–Fri 9:15 AM – 3:20 PM IST"
            return

        state["market_open"] = True
        state["in_session"]  = is_trading_session()

        # ── Gate 3: Minimum data ────────────────────────────
        candle_count = len(candle_5m_buffer)
        if candle_count < 5:
            state["signal"] = "HOLD"
            state["reason"] = f"Warming up... {candle_count}/5 candles"
            return

        # ── Compute all indicators ──────────────────────────
        _, _, ema_s       = calc_ema_candles()
        supertrend_s      = calc_supertrend()
        cpr_s, sr_s       = calc_cpr_sr_signal()
        orb_s             = calc_orb_signal()
        adx               = calc_adx_candles()
        rsi               = calc_rsi_candles()
        state["adx_value"] = adx
        update_market_bias()

        # ── Gate 4: VIX filter ──────────────────────────────
        obj = _obj[0]
        if obj:
            vix = fetch_vix(obj)
            if vix and vix > MAX_VIX:
                state["signal"] = "HOLD"
                state["reason"] = f"VIX {vix:.1f} > {MAX_VIX} — premiums too expensive"
                return

        # ── Gate 5: ADX trend filter ────────────────────────
        if adx < ADX_THRESHOLD:
            state["signal"] = "HOLD"
            state["reason"] = f"Choppy market — ADX {adx:.1f} < {ADX_THRESHOLD}"
            state["buy_votes"]  = 0
            state["sell_votes"] = 0
            return

        # ── RSI extremes filter ─────────────────────────────
        if rsi > 78:
            state["signal"] = "HOLD"
            state["reason"] = f"RSI {rsi:.1f} overbought — avoid buying CE"
            return
        if rsi < 22:
            state["signal"] = "HOLD"
            state["reason"] = f"RSI {rsi:.1f} oversold — avoid buying PE"
            return

        # ── Voting: 4 indicators, need 3/4 ─────────────────
        indicators = [ema_s, supertrend_s, cpr_s, sr_s]
        # ORB gets bonus weight when active and confirmed
        if orb_s != "NEUTRAL" and state["orb_set"]:
            indicators.append(orb_s)  # 5 votes available when ORB is active

        buy_votes  = sum(1 for s in indicators if s == "BUY")
        sell_votes = sum(1 for s in indicators if s == "SELL")
        total_v    = len(indicators)
        needed     = max(3, int(total_v * 0.6))  # 60% consensus

        state["buy_votes"]  = buy_votes
        state["sell_votes"] = sell_votes

        if buy_votes >= needed:
            raw_signal = "BUY"
        elif sell_votes >= needed:
            raw_signal = "SELL"
        else:
            raw_signal = "HOLD"

        # ── Signal history confirmation (anti-whipsaw) ──────
        update_signal_history(raw_signal)
        confirmed = get_confirmed_signal()

        # Bias alignment: only trade with market bias
        if confirmed == "BUY"  and state["market_bias"] == "BEARISH":
            confirmed = "HOLD"
            state["reason"] = "BUY signal rejected — market bias is BEARISH"
        if confirmed == "SELL" and state["market_bias"] == "BULLISH":
            confirmed = "HOLD"
            state["reason"] = "SELL signal rejected — market bias is BULLISH"

        reason = (f"{buy_votes if confirmed=='BUY' else sell_votes}/{total_v} agree "
                  f"{'BUY' if confirmed=='BUY' else 'SELL' if confirmed=='SELL' else 'mixed'} | "
                  f"ADX {adx:.1f} | RSI {rsi:.1f} | Bias {state['market_bias']}")

        state["signal"] = confirmed
        state["reason"] = reason

        if obj is None:
            return

        pos   = state["position"]
        price = state["nifty_price"]

        # ── Entry ────────────────────────────────────────────
        if pos is None:
            if confirmed in ("BUY", "SELL") and state["in_session"]:
                open_trade(obj, confirmed, price, reason, source="auto")

        # ── Exit: signal flip (with confirmation) ────────────
        else:
            if state["trade_source"] == "auto":
                flipping = (pos["type"] == "CE" and confirmed == "SELL") or \
                           (pos["type"] == "PE" and confirmed == "BUY")

                if flipping:
                    # FIX: require 2 consecutive flip signals before exiting
                    state["flip_count"] = state.get("flip_count", 0) + 1
                    if state["flip_count"] >= 2:
                        close_trade(obj, f"Confirmed signal flip → {confirmed}")
                        state["flip_count"] = 0
                    else:
                        add_log(f"Flip signal detected ({state['flip_count']}/2) — waiting for confirmation", "info")
                else:
                    state["flip_count"] = 0

    except Exception as e:
        add_log(f"Signal compute error: {e}", "error")

# ═══════════════════════════════════════
#   OPTION FUNCTIONS
# ═══════════════════════════════════════
def get_nearest_expiry():
    today = datetime.now(IST).replace(tzinfo=None)
    expiry_dates = set()
    for symbol in nfo_symbols:
        if symbol.startswith("NIFTY") and ("CE" in symbol or "PE" in symbol):
            try:
                exp = symbol[5:12]
                expiry_dates.add(exp)
            except:
                pass
    if not expiry_dates:
        days = (1 - today.weekday()) % 7 or 7
        return (today + timedelta(days=days)).strftime("%d%b%y").upper()
    future = []
    for e in expiry_dates:
        try:
            d = datetime.strptime(e, "%d%b%y")
            if d.date() >= today.date():
                future.append((d, e))
        except:
            pass
    return min(future, key=lambda x: x[0])[1] if future else min(expiry_dates)

def find_closest_symbol(price, otype):
    expiry = get_nearest_expiry()
    target_strike = round(price / 50) * 50
    for offset in range(0, 600, 50):
        for delta in ([0] if offset == 0 else [offset, -offset]):
            strike = target_strike + delta
            sym = f"NIFTY{expiry}{strike}{otype}"
            if sym in nfo_symbols:
                return sym, strike
    return None, None

def get_option_symbol(price, signal, strike_override=None):
    otype = "CE" if signal == "BUY" else "PE"
    if strike_override:
        sym = f"NIFTY{get_nearest_expiry()}{strike_override}{otype}"
        return sym
    sym, _ = find_closest_symbol(price, otype)
    if sym is None:
        strike = round(price / 50) * 50
        return f"NIFTY{get_nearest_expiry()}{strike}{otype}"
    return sym

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

# ═══════════════════════════════════════
#   OPEN TRADE (% based SL/Target)
# ═══════════════════════════════════════
def open_trade(obj, signal, price, reason, source="auto", strike_override=None, lots_override=None):
    # Check premium minimum
    symbol = get_option_symbol(price, signal, strike_override)
    ltp    = get_option_ltp(obj, symbol)
    if ltp is None:
        add_log(f"LTP not found: {symbol}", "error")
        return False, f"LTP not found for {symbol}"

    # FIX: minimum premium guard
    if ltp < MIN_PREMIUM:
        add_log(f"Premium ₹{ltp:.0f} below minimum ₹{MIN_PREMIUM} — skipping", "error")
        return False, f"Premium too low (₹{ltp:.0f})"

    # FIX: % based position sizing (10% of capital per trade)
    if lots_override:
        lots = lots_override
    else:
        max_spend = state["balance"] * CAPITAL_USE_PCT
        lots      = max(1, int(max_spend / (ltp * LOT_SIZE)))

    qty  = lots * LOT_SIZE
    cost = ltp * qty

    if cost > state["balance"]:
        add_log("Insufficient balance!", "error")
        return False, "Insufficient balance"

    # FIX: % based SL and target (not fixed points)
    sl_price  = round(ltp * (1 - SL_PCT),    2)   # 30% below entry
    tgt_price = round(ltp * (1 + TARGET_PCT), 2)   # 55% above entry

    state["balance"]      -= cost
    state["trade_source"]  = source
    state["flip_count"]    = 0
    state["position"] = {
        "symbol"       : symbol,
        "type"         : "CE" if signal == "BUY" else "PE",
        "signal"       : signal,
        "entry_price"  : ltp,
        "entry_nifty"  : price,
        "cost"         : cost,
        "lots"         : lots,
        "qty"          : qty,
        "sl_price"     : sl_price,
        "tgt_price"    : tgt_price,
        "initial_sl"   : sl_price,
        "highest_ltp"  : ltp,
        "trailing_on"  : False,
        "reason"       : reason,
        "entry_time"   : datetime.now(IST).strftime("%H:%M:%S"),
        "source"       : source,
    }
    state["trades_today"] += 1
    tag = "🤖 AUTO" if source == "auto" else "🖐 MANUAL"
    add_log(
        f"{tag} {'BUY CE' if signal=='BUY' else 'BUY PE'} {symbol} @ ₹{ltp:.2f} | "
        f"{lots} lots | SL ₹{sl_price} ({SL_PCT*100:.0f}%) | TGT ₹{tgt_price} ({TARGET_PCT*100:.0f}%)",
        "buy" if signal == "BUY" else "sell"
    )
    return True, f"Opened {symbol} @ ₹{ltp:.2f}"

# ═══════════════════════════════════════
#   CLOSE TRADE
# ═══════════════════════════════════════
def close_trade(obj, reason="Signal"):
    pos = state["position"]
    if not pos:
        return False, "No open position"
    ltp = get_option_ltp(obj, pos["symbol"])
    if ltp is None:
        add_log("Cannot close — LTP unavailable", "error")
        return False, "LTP unavailable"
    qty = pos.get("qty", LOT_SIZE)
    pnl = (ltp - pos["entry_price"]) * qty
    state["balance"]       += ltp * qty
    state["total_pnl"]     += pnl
    state["daily_pnl"]     += pnl
    state["position"]       = None
    state["trade_source"]   = "auto"
    state["flip_count"]     = 0
    if pnl >= 0:
        state["wins"]        += 1
        state["consec_losses"] = 0
    else:
        state["losses"]      += 1
        state["consec_losses"] += 1
        state["daily_loss"]  += abs(pnl)

    state["trades"].insert(0, {
        "symbol"     : pos["symbol"],
        "entry"      : pos["entry_price"],
        "exit"       : ltp,
        "pnl"        : round(pnl, 2),
        "lots"       : pos["lots"],
        "entry_time" : pos["entry_time"],
        "exit_time"  : datetime.now(IST).strftime("%H:%M:%S"),
        "reason"     : reason,
        "source"     : pos.get("source", "auto"),
    })
    add_log(
        f"EXIT {pos['symbol']} @ ₹{ltp:.2f} | "
        f"P&L {'+' if pnl>=0 else ''}₹{pnl:,.0f} | {reason}",
        "profit" if pnl >= 0 else "loss"
    )
    return True, f"Closed @ ₹{ltp:.2f} | P&L ₹{pnl:,.0f}"

# ═══════════════════════════════════════
#   LTP MONITOR — TRAILING SL (only place)
# ═══════════════════════════════════════
def ltp_monitor():
    """
    FIX: SL/target management lives HERE ONLY.
    compute_signal_and_trade() no longer duplicates this.
    Trailing SL activates after TRAIL_ACTIVATE % gain.
    """
    while True:
        try:
            pos = state["position"]
            if pos and _obj[0]:
                ltp = get_option_ltp(_obj[0], pos["symbol"])
                if ltp:
                    state["current_ltp"]    = ltp
                    state["unrealized_pnl"] = round(
                        (ltp - pos["entry_price"]) * pos["qty"], 2)

                    # Track highest LTP seen
                    if ltp > pos.get("highest_ltp", pos["entry_price"]):
                        pos["highest_ltp"] = ltp

                    # Activate trailing SL after TRAIL_ACTIVATE gain
                    gain_pct = (ltp - pos["entry_price"]) / pos["entry_price"]
                    if gain_pct >= TRAIL_ACTIVATE and not pos["trailing_on"]:
                        pos["trailing_on"] = True
                        add_log(f"Trailing SL activated at ₹{ltp:.2f} (+{gain_pct*100:.1f}%)", "info")

                    if pos["trailing_on"]:
                        # Trail SL: lock in (highest - initial_risk * 0.5)
                        trail_sl = round(pos["highest_ltp"] * (1 - SL_PCT * 0.5), 2)
                        if trail_sl > pos["sl_price"]:
                            pos["sl_price"] = trail_sl

                    # SL check
                    if ltp <= pos["sl_price"]:
                        result = "profit" if ltp > pos["entry_price"] else "loss"
                        add_log(f"SL hit @ ₹{ltp:.2f} | Entry ₹{pos['entry_price']:.2f}", result)
                        close_trade(_obj[0], "Stop Loss")

                    # Target check
                    elif ltp >= pos["tgt_price"]:
                        add_log(f"Target hit @ ₹{ltp:.2f} 🎯", "profit")
                        close_trade(_obj[0], "Target")

            else:
                state["current_ltp"]    = 0.0
                state["unrealized_pnl"] = 0.0
        except:
            pass
        time.sleep(1)

# ═══════════════════════════════════════
#   MARKET HOURS HELPERS
# ═══════════════════════════════════════
def is_market_hours():
    now = datetime.now(IST).replace(tzinfo=None)
    if now.weekday() >= 5:
        return False
    mo_start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mo_end   = now.replace(hour=15, minute=20, second=0, microsecond=0)
    return mo_start <= now <= mo_end

def is_trading_session():
    now = datetime.now(IST).replace(tzinfo=None)
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    t = h * 60 + m
    s1s = SESSION_1_START[0] * 60 + SESSION_1_START[1]
    s1e = SESSION_1_END[0]   * 60 + SESSION_1_END[1]
    s2s = SESSION_2_START[0] * 60 + SESSION_2_START[1]
    s2e = SESSION_2_END[0]   * 60 + SESSION_2_END[1]
    return (s1s <= t <= s1e) or (s2s <= t <= s2e)

def daily_reset_if_needed():
    today = datetime.now(IST).date()
    if last_reset_date[0] == today:
        return
    last_reset_date[0] = today
    state["tick_count"]    = 0
    state["micro_count"]   = 0
    state["candle_count"]  = 0
    state["trades_today"]  = 0
    state["daily_pnl"]     = 0.0
    state["daily_loss"]    = 0.0
    state["consec_losses"] = 0
    state["orb_set"]       = False
    state["orb_high"]      = 0.0
    state["orb_low"]       = 0.0
    state["signal_history"] = []
    state["flip_count"]    = 0
    state["market_bias"]   = "NEUTRAL"
    # FIX: reset manual pause on new day (auto-pauses only reset daily)
    if state.get("pause_reason","").startswith("Daily loss"):
        state["bot_paused"] = False
    tick_buffer.clear()
    micro_buffer.clear()
    candle_5m_buffer.clear()
    add_log(f"Daily reset for {today} ✓", "info")

def market_status_thread():
    while True:
        try:
            now   = datetime.now(IST).replace(tzinfo=None)
            open_ = is_market_hours()
            state["market_open"] = open_
            state["last_update"] = now.strftime("%H:%M:%S")
            if open_:
                daily_reset_if_needed()
            else:
                if state["signal"] not in ("BOT PAUSED",):
                    state["signal"] = "MARKET CLOSED"
                    state["reason"] = "Mon–Fri 9:15 AM – 3:20 PM IST"
        except:
            pass
        time.sleep(1)

# ═══════════════════════════════════════
#   BOT LOOP
# ═══════════════════════════════════════
def bot_loop():
    add_log("NSEAlgo v5 — Power S&R + ORB + Supertrend Bot starting...", "info")
    threading.Thread(target=market_status_thread, daemon=True).start()
    obj = connect_angel()
    if obj is None:
        add_log("Cannot start — login failed", "error")
        return
    load_symbol_master()

    while True:
        try:
            if not is_market_hours():
                state["ws_connected"] = False
                add_log("Outside market hours — waiting for 9:15 AM IST", "info")
                while not is_market_hours():
                    time.sleep(30)
                add_log("Market opening — reconnecting Angel One...", "info")

            obj = connect_angel()
            if obj is None:
                time.sleep(30)
                continue

            add_log("Market open — fetching daily levels...", "info")
            fetch_daily_levels(obj)
            daily_reset_if_needed()

            token_list = [{"exchangeType": NIFTY_EXCH_TYPE, "tokens": [NIFTY_TOKEN]}]
            _ws_token_list[0] = token_list

            ws = SmartWebSocketV2(
                _auth_token[0], API_KEY.strip(),
                CLIENT_CODE.strip(), _feed_token[0]
            )
            _ws_ref[0]  = ws
            ws.on_open  = on_open
            ws.on_data  = on_data
            ws.on_error = on_error
            ws.on_close = on_close

            state["last_tick_time"] = time.time()

            def run_ws():
                try:
                    ws.connect()
                except Exception as e:
                    add_log(f"WS run error: {e}", "error")
                    state["ws_connected"] = False

            ws_thread = threading.Thread(target=run_ws, daemon=True)
            ws_thread.start()
            time.sleep(20)

            while is_market_hours():
                time.sleep(1)
                secs_since = time.time() - state["last_tick_time"]
                if (not state["ws_connected"]) or secs_since > 60:
                    add_log(f"WS zombie ({secs_since:.0f}s since tick) — reconnecting", "error")
                    break

            try:
                ws.close_connection()
            except:
                pass

            if not is_market_hours() and state["position"]:
                close_trade(obj, "Market Close")
            if not is_market_hours():
                add_log("Market closed for today ✓", "info")

        except Exception as e:
            add_log(f"Bot loop error: {e}", "error")
            time.sleep(30)

# ═══════════════════════════════════════
#   API ROUTES
# ═══════════════════════════════════════
@app.route("/api/state")
def api_state():
    total = state["wins"] + state["losses"]
    pos   = state["position"]
    max_daily = CAPITAL * MAX_DAILY_LOSS_PCT
    return jsonify({
        "connected"      : state["connected"],
        "ws_connected"   : state["ws_connected"],
        "market_open"    : state["market_open"],
        "nifty_price"    : state["nifty_price"],
        "signal"         : state["signal"],
        "reason"         : state["reason"],
        "balance"        : round(state["balance"], 2),
        "total_pnl"      : round(state["total_pnl"], 2),
        "daily_pnl"      : round(state["daily_pnl"], 2),
        "daily_loss"     : round(state["daily_loss"], 2),
        "daily_loss_pct" : round(state["daily_loss"] / max_daily * 100, 1),
        "max_daily_loss" : max_daily,
        "pnl_pct"        : round(state["total_pnl"] / CAPITAL * 100, 2) if CAPITAL else 0,
        "wins"           : state["wins"],
        "losses"         : state["losses"],
        "winrate"        : round(state["wins"] / total * 100, 1) if total > 0 else 0,
        "consec_losses"  : state["consec_losses"],
        "trades_today"   : state["trades_today"],
        "trades_left"    : max(0, MAX_TRADES_PER_DAY - state["trades_today"]),
        "position"       : pos,
        "trades"         : state["trades"][:25],
        "log"            : state["log"][:40],
        "last_update"    : state["last_update"],
        "ema9"           : state.get("ema9", 0.0),
        "ema21"          : state.get("ema21", 0.0),
        "adx"            : state.get("adx_value", 0.0),
        "vix"            : state.get("vix", 0.0),
        "rsi"            : calc_rsi_candles(),
        "cpr_p"          : state.get("cpr_p", 0.0),
        "cpr_tc"         : state.get("cpr_tc", 0.0),
        "cpr_bc"         : state.get("cpr_bc", 0.0),
        "r1":state.get("r1",0),"r2":state.get("r2",0),
        "r3":state.get("r3",0),"r4":state.get("r4",0),
        "s1":state.get("s1",0),"s2":state.get("s2",0),
        "s3":state.get("s3",0),"s4":state.get("s4",0),
        "pdh"            : state.get("pdh", 0.0),
        "pdl"            : state.get("pdl", 0.0),
        "orb_high"       : state.get("orb_high", 0.0),
        "orb_low"        : state.get("orb_low", 0.0),
        "orb_set"        : state.get("orb_set", False),
        "orb_signal"     : state.get("orb_signal", "—"),
        "support"        : state.get("support", 0.0),
        "resistance"     : state.get("resistance", 0.0),
        "ema_signal"     : state.get("ema_signal", "—"),
        "cpr_signal"     : state.get("cpr_signal", "—"),
        "sr_signal"      : state.get("sr_signal", "—"),
        "supertrend"     : state.get("vwap_signal", "—"),
        "market_bias"    : state.get("market_bias", "NEUTRAL"),
        "buy_votes"      : state.get("buy_votes", 0),
        "sell_votes"     : state.get("sell_votes", 0),
        "in_session"     : state.get("in_session", False),
        "tick_count"     : state.get("tick_count", 0),
        "micro_count"    : state.get("micro_count", 0),
        "candle_count"   : state.get("candle_count", 0),
        "capital"        : CAPITAL,
        "sl_price"       : round(pos["sl_price"],  2) if pos else 0,
        "tgt_price"      : round(pos["tgt_price"], 2) if pos else 0,
        "unrealized"     : state.get("unrealized_pnl", 0.0),
        "current_ltp"    : state.get("current_ltp", 0.0),
        "bot_paused"     : state.get("bot_paused", False),
        "trade_source"   : state.get("trade_source", "auto"),
        "flip_count"     : state.get("flip_count", 0),
        "signal_history" : state.get("signal_history", []),
        "trailing_on"    : pos.get("trailing_on", False) if pos else False,
        "highest_ltp"    : pos.get("highest_ltp", 0) if pos else 0,
    })

@app.route("/api/bot/toggle", methods=["POST"])
def api_bot_toggle():
    # Don't allow manual resume if auto-paused by daily loss
    if state.get("pause_reason","").startswith("Daily loss") and state["bot_paused"]:
        return jsonify({"ok": False, "message": "Cannot resume — daily loss limit reached"})
    state["bot_paused"] = not state["bot_paused"]
    if not state["bot_paused"]:
        state["consec_losses"] = 0  # reset on manual resume
    status = "PAUSED" if state["bot_paused"] else "RESUMED"
    add_log(f"Bot {status} by user", "info")
    return jsonify({"ok": True, "bot_paused": state["bot_paused"], "message": f"Bot {status}"})

@app.route("/api/manual/buy_ce", methods=["POST"])
def api_manual_buy_ce():
    obj = _obj[0]
    if obj is None:
        return jsonify({"ok": False, "message": "Not connected to Angel One"})
    if state["position"]:
        return jsonify({"ok": False, "message": "Position already open — close it first"})
    data   = request.get_json(silent=True) or {}
    strike = data.get("strike")
    lots   = data.get("lots")
    price  = state["nifty_price"]
    if not price:
        return jsonify({"ok": False, "message": "No live price available"})
    try:
        strike = int(strike) if strike else None
        lots   = int(lots)   if lots   else None
    except:
        return jsonify({"ok": False, "message": "Invalid strike or lots"})
    ok, msg = open_trade(obj, "BUY", price, "Manual CE buy",
                         source="manual", strike_override=strike, lots_override=lots)
    return jsonify({"ok": ok, "message": msg})

@app.route("/api/manual/buy_pe", methods=["POST"])
def api_manual_buy_pe():
    obj = _obj[0]
    if obj is None:
        return jsonify({"ok": False, "message": "Not connected to Angel One"})
    if state["position"]:
        return jsonify({"ok": False, "message": "Position already open — close it first"})
    data   = request.get_json(silent=True) or {}
    strike = data.get("strike")
    lots   = data.get("lots")
    price  = state["nifty_price"]
    if not price:
        return jsonify({"ok": False, "message": "No live price available"})
    try:
        strike = int(strike) if strike else None
        lots   = int(lots)   if lots   else None
    except:
        return jsonify({"ok": False, "message": "Invalid strike or lots"})
    ok, msg = open_trade(obj, "SELL", price, "Manual PE buy",
                         source="manual", strike_override=strike, lots_override=lots)
    return jsonify({"ok": ok, "message": msg})

@app.route("/api/manual/close", methods=["POST"])
def api_manual_close():
    obj = _obj[0]
    if obj is None:
        return jsonify({"ok": False, "message": "Not connected"})
    if not state["position"]:
        return jsonify({"ok": False, "message": "No open position"})
    ok, msg = close_trade(obj, "Manual close")
    return jsonify({"ok": ok, "message": msg})

# ═══════════════════════════════════════
#   DASHBOARD HTML
# ═══════════════════════════════════════
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NSEAlgo v5 — Smart Options Bot</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Rajdhani:wght@500;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0b0f1a;--bg2:#111827;--bg3:#1a2235;--bg4:#1f2d42;
  --border:#1e3a5f;--border2:#2a4a70;
  --text:#e2e8f0;--muted:#64748b;--dim:#374151;
  --green:#10b981;--green-bg:rgba(16,185,129,.12);--green-border:rgba(16,185,129,.25);
  --red:#ef4444;--red-bg:rgba(239,68,68,.12);--red-border:rgba(239,68,68,.25);
  --amber:#f59e0b;--amber-bg:rgba(245,158,11,.1);--amber-border:rgba(245,158,11,.2);
  --blue:#3b82f6;--blue-bg:rgba(59,130,246,.1);--blue-border:rgba(59,130,246,.2);
  --purple:#8b5cf6;--purple-bg:rgba(139,92,246,.1);--purple-border:rgba(139,92,246,.2);
  --cyan:#06b6d4;
}
body{background:var(--bg);color:var(--text);font-family:'Space Mono',monospace;min-height:100vh;font-size:12px}
.hdr{background:var(--bg2);border-bottom:1px solid var(--border);padding:10px 20px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:99;gap:8px;flex-wrap:wrap}
.logo{font-family:'Rajdhani',sans-serif;font-size:20px;font-weight:700;letter-spacing:.1em;color:#fff}
.logo span{color:var(--cyan)}
.wrap{max-width:1100px;margin:0 auto;padding:14px 14px 60px}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px}
.g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:10px}
.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:10px}
.g5{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:10px}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.card-title{font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:var(--muted);margin-bottom:10px;font-weight:700}
.big{font-family:'Rajdhani',sans-serif;font-size:28px;font-weight:700;letter-spacing:-.01em}
.med{font-family:'Rajdhani',sans-serif;font-size:20px;font-weight:700}
.live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);display:inline-block;animation:pulse 1.4s infinite;margin-right:5px}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.8)}}
.pill{display:inline-flex;align-items:center;padding:3px 10px;border-radius:99px;font-size:10px;font-weight:700;letter-spacing:.06em}
.p-g{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.p-r{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border)}
.p-a{background:var(--amber-bg);color:var(--amber);border:1px solid var(--amber-border)}
.p-b{background:var(--blue-bg);color:var(--blue);border:1px solid var(--blue-border)}
.p-p{background:var(--purple-bg);color:var(--purple);border:1px solid var(--purple-border)}
.p-dim{background:var(--bg3);color:var(--muted);border:1px solid var(--border)}
.sig-box{padding:10px 16px;border-radius:8px;display:inline-flex;align-items:center;gap:8px;font-family:'Rajdhani',sans-serif;font-size:18px;font-weight:700;letter-spacing:.06em}
.sb-buy{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.sb-sell{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border)}
.sb-hold{background:var(--bg3);color:var(--muted);border:1px solid var(--border)}
.sb-pause{background:var(--purple-bg);color:var(--purple);border:1px solid var(--purple-border)}
.sb-closed{background:var(--amber-bg);color:var(--amber);border:1px solid var(--amber-border)}
.row{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--border);font-size:11px}
.row:last-child{border-bottom:none}
.rl{color:var(--muted)}
.rv{font-weight:700;color:var(--text)}
.g{color:var(--green)}.r{color:var(--red)}.a{color:var(--amber)}.b{color:var(--blue)}.p{color:var(--purple)}.c{color:var(--cyan)}
.metric{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:10px 12px}
.mlbl{font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:4px}
.mval{font-family:'Rajdhani',sans-serif;font-size:18px;font-weight:700}
.vote-bar{display:flex;gap:8px;margin-top:10px}
.vote{flex:1;padding:6px 8px;border-radius:6px;text-align:center;font-size:11px;font-weight:700;font-family:'Rajdhani',sans-serif}
.vg{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.vr{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border)}
.vd{background:var(--bg3);color:var(--muted);border:1px solid var(--border)}
.ind-row{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid var(--border);font-size:11px}
.ind-row:last-child{border-bottom:none}
.ind-lbl{color:var(--cyan);font-weight:700;font-size:11px}
.ind-val{text-align:right;font-weight:700;max-width:55%;word-break:break-word}
.log-item{padding:5px 8px;border-radius:5px;font-size:10px;margin-bottom:3px;border-left:2px solid var(--border);background:var(--bg3);line-height:1.5}
.lb{border-left-color:var(--green);color:var(--green)}
.ls{border-left-color:var(--red);color:var(--red)}
.lp{border-left-color:var(--green)}
.ll{border-left-color:var(--red)}
.lsc{border-left-color:var(--green)}
.le{border-left-color:var(--red);color:var(--red)}
.li{border-left-color:var(--border2);color:var(--muted)}
.lt{color:var(--muted);margin-right:6px}
.lm{color:var(--text)}
table{width:100%;border-collapse:collapse}
th{color:var(--muted);font-size:9px;letter-spacing:.12em;padding:5px 8px;text-align:left;border-bottom:1px solid var(--border);text-transform:uppercase}
td{padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;font-weight:700}
.pcard{border-color:var(--amber-border);background:rgba(245,158,11,.04)}
.pcard-m{border-color:var(--purple-border);background:rgba(139,92,246,.04)}
.manual-panel{background:var(--bg2);border:1px solid var(--purple-border);border-radius:10px;padding:16px;margin-bottom:10px}
.mtitle{font-size:9px;letter-spacing:.15em;color:var(--purple);text-transform:uppercase;font-weight:700;margin-bottom:14px}
.mrow{display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap}
.ilbl{font-size:9px;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px;font-weight:700}
.inp{background:var(--bg3);border:1px solid var(--border2);border-radius:6px;padding:7px 10px;color:var(--text);font-family:'Space Mono',monospace;font-size:12px;width:110px;outline:none}
.inp:focus{border-color:var(--purple)}
.btn{padding:8px 16px;border-radius:7px;border:none;font-family:'Space Mono',monospace;font-size:11px;font-weight:700;cursor:pointer;transition:all .15s;letter-spacing:.04em;white-space:nowrap}
.btn:active{transform:scale(.97)}
.btn:disabled{opacity:.3;cursor:not-allowed;transform:none}
.btn-ce{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.btn-ce:hover:not(:disabled){background:rgba(16,185,129,.25)}
.btn-pe{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border)}
.btn-pe:hover:not(:disabled){background:rgba(239,68,68,.25)}
.btn-cl{background:var(--amber-bg);color:var(--amber);border:1px solid var(--amber-border)}
.btn-cl:hover:not(:disabled){background:rgba(245,158,11,.25)}
.btn-tog{padding:6px 14px;border-radius:6px;border:none;font-family:'Space Mono',monospace;font-size:10px;font-weight:700;cursor:pointer;transition:all .15s}
.tog-pause{background:var(--purple-bg);color:var(--purple);border:1px solid var(--purple-border)}
.tog-resume{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.toast{position:fixed;bottom:24px;right:24px;padding:10px 18px;border-radius:8px;font-size:12px;font-weight:700;z-index:9999;opacity:0;transform:translateY(8px);transition:all .25s;pointer-events:none;max-width:320px;box-shadow:0 4px 20px rgba(0,0,0,.4)}
.toast.show{opacity:1;transform:translateY(0)}
.t-ok{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.t-err{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border)}
.dloss-bar{height:5px;border-radius:3px;background:var(--bg4);margin-top:6px;overflow:hidden}
.dloss-fill{height:100%;border-radius:3px;background:var(--green);transition:width .5s,background .5s}
.sig-hist{display:flex;gap:4px;margin-top:8px;flex-wrap:wrap}
.sh-item{width:22px;height:22px;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700}
.sh-buy{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.sh-sell{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border)}
.sh-hold{background:var(--bg3);color:var(--muted);border:1px solid var(--border)}
.orb-badge{display:inline-flex;align-items:center;gap:5px;padding:3px 8px;border-radius:5px;font-size:10px;font-weight:700;background:var(--cyan-bg,rgba(6,182,212,.1));color:var(--cyan);border:1px solid rgba(6,182,212,.25)}
.sep{width:100%;height:1px;background:var(--border);margin:10px 0}
.src-auto{background:var(--blue-bg);color:var(--blue);border:1px solid var(--blue-border);padding:2px 7px;border-radius:4px;font-size:9px;font-weight:700}
.src-man{background:var(--purple-bg);color:var(--purple);border:1px solid var(--purple-border);padding:2px 7px;border-radius:4px;font-size:9px;font-weight:700}
@media(max-width:700px){.g4{grid-template-columns:1fr 1fr}.g3{grid-template-columns:1fr 1fr}.g5{grid-template-columns:repeat(3,1fr)}.g2{grid-template-columns:1fr}.big{font-size:22px}.mrow{flex-direction:column}.inp{width:100%}}
</style>
</head>
<body>
<div class="hdr">
  <div class="logo">NSE<span>Algo</span> <span style="font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;font-weight:400">v5</span></div>
  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
    <span id="li" style="font-size:10px;color:var(--muted)">Connecting...</span>
    <span id="ws" class="pill p-dim">WS Off</span>
    <span id="sess" class="pill p-dim" style="display:none">—</span>
    <span id="vix-pill" class="pill p-dim">VIX —</span>
    <button id="bot-tog" class="btn-tog tog-pause" onclick="toggleBot()">⏸ Pause</button>
    <span id="upd" style="font-size:10px;color:var(--muted)">—</span>
  </div>
</div>

<div class="wrap">

  <!-- Row 1: Price + Signal -->
  <div class="g2">
    <div class="card">
      <div class="card-title">NIFTY 50 · Live feed</div>
      <div class="big c" id="np">₹—</div>
      <div style="margin-top:8px;display:flex;gap:14px;flex-wrap:wrap;font-size:11px">
        <span>EMA9 <span id="e9" class="a">—</span></span>
        <span>EMA21 <span id="e21" class="p">—</span></span>
        <span>ADX <span id="adx" class="c">—</span></span>
        <span>RSI <span id="rsi" class="b">—</span></span>
      </div>
      <div style="margin-top:6px;display:flex;gap:10px;flex-wrap:wrap;font-size:10px;color:var(--muted)">
        <span>Ticks <span class="g" id="tc">0</span></span>
        <span>5m candles <span class="a" id="cc">0</span></span>
        <span>Bias <span id="bias" class="c">NEUTRAL</span></span>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Signal · 60% consensus vote</div>
      <div id="sb"><span class="sig-box sb-hold">— WAITING</span></div>
      <div style="margin-top:6px;font-size:10px;color:var(--muted)" id="sr">—</div>
      <div class="vote-bar">
        <div class="vote vg" id="bv">BUY 0</div>
        <div class="vote vd" id="nv">HOLD</div>
        <div class="vote vr" id="sv">SELL 0</div>
      </div>
      <div style="margin-top:8px;font-size:10px;color:var(--muted)">Signal history (last 5)</div>
      <div class="sig-hist" id="shist"></div>
    </div>
  </div>

  <!-- Row 2: PnL metrics -->
  <div class="g5">
    <div class="metric"><div class="mlbl">Balance</div><div class="mval" id="bal">₹—</div></div>
    <div class="metric"><div class="mlbl">Total P&L</div><div class="mval" id="pnl">₹0</div><div style="font-size:10px;margin-top:2px" id="pnlp">0.00%</div></div>
    <div class="metric"><div class="mlbl">Daily P&L</div><div class="mval" id="dpnl">₹0</div></div>
    <div class="metric"><div class="mlbl">Win rate</div><div class="mval a" id="wr">0%</div><div style="font-size:10px;color:var(--muted);margin-top:2px" id="trd">0 trades</div></div>
    <div class="metric">
      <div class="mlbl">Daily risk used</div>
      <div style="display:flex;justify-content:space-between;align-items:center">
        <span class="mval" id="dl">₹0</span>
        <span style="font-size:10px;color:var(--muted)" id="dlp">0%</span>
      </div>
      <div class="dloss-bar"><div class="dloss-fill" id="dlbar" style="width:0%"></div></div>
    </div>
  </div>

  <!-- Row 3: Risk stats -->
  <div class="g4" style="margin-bottom:10px">
    <div class="metric"><div class="mlbl">Wins / Losses</div><div style="display:flex;gap:8px;align-items:baseline"><span class="mval g" id="wi">0</span><span style="color:var(--dim)">/</span><span class="mval r" id="lo">0</span></div></div>
    <div class="metric"><div class="mlbl">Consec losses</div><div class="mval" id="cl">0</div></div>
    <div class="metric"><div class="mlbl">Trades today</div><div class="mval" id="tday">0</div><div style="font-size:10px;color:var(--muted);margin-top:2px" id="tleft">— left</div></div>
    <div class="metric"><div class="mlbl">Unrealized P&L</div><div class="mval" id="unr">₹—</div></div>
  </div>

  <!-- Manual panel -->
  <div class="manual-panel">
    <div class="mtitle">🖐 Manual Trading Override</div>
    <div class="mrow">
      <div><div class="ilbl">Strike (opt)</div><input class="inp" id="m-strike" type="number" placeholder="e.g. 24500" step="50"></div>
      <div><div class="ilbl">Lots</div><input class="inp" id="m-lots" type="number" placeholder="e.g. 2" min="1" style="width:80px"></div>
      <button class="btn btn-ce" id="btn-ce" onclick="manualBuyCE()">▲ Buy CE</button>
      <button class="btn btn-pe" id="btn-pe" onclick="manualBuyPE()">▼ Buy PE</button>
      <button class="btn btn-cl" id="btn-cl" onclick="manualClose()" disabled>✕ Close</button>
    </div>
    <div id="m-hint" style="font-size:10px;color:var(--muted);margin-top:10px">Leave strike blank for ATM. Leave lots blank for auto-sizing (10% of capital).</div>
  </div>

  <!-- Levels + Indicators -->
  <div class="g2">
    <div class="card">
      <div class="card-title">Key levels · CPR + Pivot + ORB</div>
      <div class="row"><span class="rl">R4</span><span class="rv r" id="r4">—</span></div>
      <div class="row"><span class="rl">R3</span><span class="rv r" id="r3">—</span></div>
      <div class="row"><span class="rl">R2</span><span class="rv r" id="r2">—</span></div>
      <div class="row"><span class="rl">R1</span><span class="rv r" id="r1">—</span></div>
      <div class="row"><span class="rl">PDH</span><span class="rv r" id="pdh">—</span></div>
      <div class="row" style="background:rgba(59,130,246,.06);padding:6px 8px;border-radius:5px;margin:3px 0">
        <span class="rl">CPR Top</span><span class="rv b" id="tc">—</span></div>
      <div class="row" style="background:rgba(59,130,246,.1);padding:6px 8px;border-radius:5px;margin:3px 0">
        <span class="rl b" style="font-weight:700">PIVOT</span><span class="rv b" style="font-size:13px" id="pp">—</span></div>
      <div class="row" style="background:rgba(59,130,246,.06);padding:6px 8px;border-radius:5px;margin:3px 0">
        <span class="rl">CPR Bot</span><span class="rv b" id="bc">—</span></div>
      <div class="row"><span class="rl">PDL</span><span class="rv g" id="pdl">—</span></div>
      <div class="row"><span class="rl">S1</span><span class="rv g" id="s1">—</span></div>
      <div class="row"><span class="rl">S2</span><span class="rv g" id="s2">—</span></div>
      <div class="row"><span class="rl">S3</span><span class="rv g" id="s3">—</span></div>
      <div class="row"><span class="rl">S4</span><span class="rv g" id="s4">—</span></div>
      <div class="sep"></div>
      <div class="row"><span class="rl c">ORB High</span><span id="orb-h" class="rv c">—</span></div>
      <div class="row"><span class="rl c">ORB Low</span><span id="orb-l" class="rv c">—</span></div>
      <div style="margin-top:6px"><span id="orb-sig" class="pill p-dim">ORB: building</span></div>
    </div>
    <div class="card">
      <div class="card-title">Indicator signals · 5-min candles</div>
      <div class="ind-row"><span class="ind-lbl">EMA 9/21</span><span id="ema-s" class="ind-val">—</span></div>
      <div class="ind-row"><span class="ind-lbl">Supertrend</span><span id="st-s" class="ind-val">—</span></div>
      <div class="ind-row"><span class="ind-lbl">CPR Position</span><span id="cpr-s" class="ind-val">—</span></div>
      <div class="ind-row"><span class="ind-lbl">S&R Zones</span><span id="sr-s" class="ind-val">—</span></div>
      <div class="ind-row"><span class="ind-lbl">ORB</span><span id="orb-s" class="ind-val">—</span></div>
      <div class="sep"></div>
      <div class="row"><span class="rl">Support zone</span><span class="rv g" id="sup">—</span></div>
      <div class="row"><span class="rl">Resistance zone</span><span class="rv r" id="res">—</span></div>
      <div class="row"><span class="rl">Trailing SL on</span><span id="trail-on" class="rv">—</span></div>
      <div class="row"><span class="rl">Highest LTP</span><span id="hltp" class="rv a">—</span></div>
      <div class="row"><span class="rl">Current SL</span><span class="rv r" id="csl">—</span></div>
      <div class="row"><span class="rl">Current TGT</span><span class="rv g" id="ctgt">—</span></div>
      <div class="row"><span class="rl">Flip count</span><span id="flp" class="rv a">0/2</span></div>
    </div>
  </div>

  <!-- Open position -->
  <div class="card pcard" id="pc" style="margin-bottom:10px;display:none">
    <div class="card-title" id="pc-title">📂 Open Position</div>
    <div id="pg" class="g3" style="gap:8px"></div>
  </div>
  <div style="text-align:center;padding:12px;color:var(--muted);font-size:11px;margin-bottom:10px" id="npc">
    No open position — auto entries only during session windows
  </div>

  <!-- Trades + Log -->
  <div class="g2">
    <div class="card">
      <div class="card-title">Trade history</div>
      <div id="te" style="color:var(--muted);padding:10px 0;font-size:11px">No trades yet</div>
      <table id="tt" style="display:none">
        <thead><tr><th>Symbol</th><th>Lots</th><th>P&L</th><th>Src</th><th>Reason</th><th>Time</th></tr></thead>
        <tbody id="tb"></tbody>
      </table>
    </div>
    <div class="card">
      <div class="card-title">Activity log</div>
      <div id="lc" style="max-height:340px;overflow-y:auto"></div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>
<script>
const fi = n => new Intl.NumberFormat('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2}).format(n);
const fi0 = n => new Intl.NumberFormat('en-IN',{minimumFractionDigits:0,maximumFractionDigits:0}).format(n);

function showToast(msg, ok=true){
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = 'toast show ' + (ok?'t-ok':'t-err');
  clearTimeout(t._tid); t._tid = setTimeout(()=>t.className='toast',3500);
}

async function apiFetch(url, body=null){
  const r = await fetch(url, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
  return r.json();
}

async function toggleBot(){
  const d = await apiFetch('/api/bot/toggle');
  showToast(d.message, d.ok);
}

function setLoading(on){
  ['btn-ce','btn-pe','btn-cl'].forEach(id=>{
    const el = document.getElementById(id);
    if(el) el.disabled = on;
  });
}

async function manualBuyCE(){
  const strike = document.getElementById('m-strike').value;
  const lots   = document.getElementById('m-lots').value;
  const body   = {};
  if(strike) body.strike = parseInt(strike);
  if(lots)   body.lots   = parseInt(lots);
  setLoading(true);
  const d = await apiFetch('/api/manual/buy_ce', body);
  showToast(d.message, d.ok); setLoading(false);
}

async function manualBuyPE(){
  const strike = document.getElementById('m-strike').value;
  const lots   = document.getElementById('m-lots').value;
  const body   = {};
  if(strike) body.strike = parseInt(strike);
  if(lots)   body.lots   = parseInt(lots);
  setLoading(true);
  const d = await apiFetch('/api/manual/buy_pe', body);
  showToast(d.message, d.ok); setLoading(false);
}

async function manualClose(){
  if(!confirm('Close position at market price?')) return;
  setLoading(true);
  const d = await apiFetch('/api/manual/close');
  showToast(d.message, d.ok); setLoading(false);
}

function colClass(val){ return val>0?'g':val<0?'r':'a'; }
function sigColor(s){ return s==='BUY'?'g':s==='SELL'?'r':'a'; }

async function tick(){
  try{
    const d = await fetch('/api/state').then(r=>r.json());
    const hasPos = !!d.position;

    // Header
    document.getElementById('li').innerHTML = d.connected?'<span class="live-dot"></span>Live':'Connecting...';
    document.getElementById('upd').textContent = d.last_update;

    const wsEl = document.getElementById('ws');
    wsEl.className = d.ws_connected?'pill p-g':'pill p-r';
    wsEl.textContent = d.ws_connected?'WS Live':'WS Off';

    const vixEl = document.getElementById('vix-pill');
    const vix = d.vix||0;
    vixEl.textContent = 'VIX ' + (vix?vix.toFixed(1):'—');
    vixEl.className = 'pill ' + (vix>18?'p-r':vix>14?'p-a':'p-g');

    const sess = document.getElementById('sess');
    sess.style.display='inline-flex';
    if(d.in_session){sess.className='pill p-g';sess.textContent='Session On';}
    else if(d.market_open){sess.className='pill p-a';sess.textContent='Out of Session';}
    else{sess.className='pill p-r';sess.textContent='Market Closed';}

    const btn = document.getElementById('bot-tog');
    btn.className = 'btn-tog ' + (d.bot_paused?'tog-resume':'tog-pause');
    btn.textContent = d.bot_paused?'▶ Resume':'⏸ Pause';

    // Price
    document.getElementById('np').textContent  = '₹' + fi(d.nifty_price);
    document.getElementById('e9').textContent  = d.ema9  || '—';
    document.getElementById('e21').textContent = d.ema21 || '—';
    document.getElementById('adx').textContent = d.adx   || '—';
    document.getElementById('rsi').textContent = d.rsi   || '—';
    document.getElementById('tc').textContent  = d.tick_count;
    document.getElementById('cc').textContent  = d.candle_count;
    document.getElementById('bias').textContent = d.market_bias||'—';
    document.getElementById('bias').className   = d.market_bias==='BULLISH'?'g':d.market_bias==='BEARISH'?'r':'c';

    // Signal
    const s = d.signal;
    let sc = 'sb-hold', ic = '—';
    if(s==='BUY'){sc='sb-buy';ic='▲';}
    else if(s==='SELL'){sc='sb-sell';ic='▼';}
    else if(s==='MARKET CLOSED'){sc='sb-closed';ic='💤';}
    else if(s==='AUTO-PAUSED'||s==='BOT PAUSED'){sc='sb-pause';ic='⏸';}
    document.getElementById('sb').innerHTML = `<span class="sig-box ${sc}">${ic} ${s}</span>`;
    document.getElementById('sr').textContent = d.reason;
    document.getElementById('bv').textContent = 'BUY ' + d.buy_votes;
    document.getElementById('sv').textContent = 'SELL ' + d.sell_votes;

    // Signal history pills
    const hist = d.signal_history||[];
    document.getElementById('shist').innerHTML = hist.map(h=>
      `<div class="sh-item ${h==='BUY'?'sh-buy':h==='SELL'?'sh-sell':'sh-hold'}">${h==='BUY'?'B':h==='SELL'?'S':'H'}</div>`
    ).join('');

    // Metrics
    document.getElementById('bal').textContent = '₹'+fi0(d.balance);
    const pnlEl = document.getElementById('pnl');
    pnlEl.textContent = (d.total_pnl>=0?'+':'')+'₹'+fi0(Math.abs(d.total_pnl));
    pnlEl.className = 'mval ' + colClass(d.total_pnl);
    document.getElementById('pnlp').textContent = (d.pnl_pct>=0?'+':'')+d.pnl_pct+'%';
    document.getElementById('pnlp').style.color = d.pnl_pct>=0?'var(--green)':'var(--red)';
    const dpEl = document.getElementById('dpnl');
    dpEl.textContent = (d.daily_pnl>=0?'+':'')+'₹'+fi0(Math.abs(d.daily_pnl));
    dpEl.className = 'mval ' + colClass(d.daily_pnl);
    document.getElementById('wr').textContent = d.winrate+'%';
    document.getElementById('trd').textContent = (d.wins+d.losses)+' trades';
    document.getElementById('wi').textContent  = d.wins;
    document.getElementById('lo').textContent  = d.losses;

    const dlEl = document.getElementById('dl');
    dlEl.textContent = '₹'+fi0(d.daily_loss);
    dlEl.className = 'mval ' + (d.daily_loss_pct>75?'r':d.daily_loss_pct>50?'a':'g');
    document.getElementById('dlp').textContent = d.daily_loss_pct+'%';
    const fill = document.getElementById('dlbar');
    fill.style.width = Math.min(d.daily_loss_pct,100)+'%';
    fill.style.background = d.daily_loss_pct>75?'var(--red)':d.daily_loss_pct>50?'var(--amber)':'var(--green)';

    const clEl = document.getElementById('cl');
    clEl.textContent = d.consec_losses;
    clEl.className = 'mval ' + (d.consec_losses>=2?'r':d.consec_losses>=1?'a':'g');
    document.getElementById('tday').textContent = d.trades_today;
    document.getElementById('tleft').textContent = d.trades_left + ' left today';
    const unrEl = document.getElementById('unr');
    unrEl.textContent = (d.unrealized>=0?'+':'')+'₹'+fi0(Math.abs(d.unrealized));
    unrEl.className = 'mval ' + colClass(d.unrealized);

    // Levels
    ['r4','r3','r2','r1','s1','s2','s3','s4'].forEach(k=>{
      const el = document.getElementById(k);
      if(el) el.textContent = d[k]?'₹'+fi(d[k]):'—';
    });
    document.getElementById('pdh').textContent = d.pdh?'₹'+fi(d.pdh):'—';
    document.getElementById('pdl').textContent = d.pdl?'₹'+fi(d.pdl):'—';
    document.getElementById('tc').textContent  = d.cpr_tc?'₹'+fi(d.cpr_tc):'—';
    document.getElementById('pp').textContent  = d.cpr_p ?'₹'+fi(d.cpr_p) :'—';
    document.getElementById('bc').textContent  = d.cpr_bc?'₹'+fi(d.cpr_bc):'—';
    document.getElementById('orb-h').textContent = d.orb_high?'₹'+fi(d.orb_high):'—';
    document.getElementById('orb-l').textContent = d.orb_low ?'₹'+fi(d.orb_low) :'—';
    const orbSig = document.getElementById('orb-sig');
    orbSig.textContent = d.orb_set?d.orb_signal:'ORB: building (need 9:30 AM)';

    // Indicators
    function indHTML(val){
      const c = val.includes('BUY')?'g':val.includes('SELL')?'r':'a';
      return `<span class="${c}">${val}</span>`;
    }
    document.getElementById('ema-s').innerHTML = indHTML(d.ema_signal||'—');
    document.getElementById('st-s').innerHTML  = indHTML(d.supertrend||'—');
    document.getElementById('cpr-s').innerHTML = indHTML(d.cpr_signal||'—');
    document.getElementById('sr-s').innerHTML  = indHTML(d.sr_signal||'—');
    document.getElementById('orb-s').innerHTML = indHTML(d.orb_signal||'—');
    document.getElementById('sup').textContent  = d.support?'₹'+fi(d.support):'—';
    document.getElementById('res').textContent  = d.resistance?'₹'+fi(d.resistance):'—';
    document.getElementById('trail-on').innerHTML = d.trailing_on
      ? '<span class="g">YES — trailing active</span>' : '<span class="a">No</span>';
    document.getElementById('hltp').textContent  = d.highest_ltp?'₹'+fi(d.highest_ltp):'—';
    document.getElementById('csl').textContent   = d.sl_price?'₹'+fi(d.sl_price):'—';
    document.getElementById('ctgt').textContent  = d.tgt_price?'₹'+fi(d.tgt_price):'—';
    document.getElementById('flp').textContent   = (d.flip_count||0)+'/2';

    // Manual buttons
    document.getElementById('btn-ce').disabled = hasPos;
    document.getElementById('btn-pe').disabled = hasPos;
    document.getElementById('btn-cl').disabled = !hasPos;
    const hint = document.getElementById('m-hint');
    if(hasPos) hint.innerHTML='<span class="a">⚠ Position open — close before new trade</span>';
    else if(d.bot_paused) hint.innerHTML='<span class="p">🖐 Bot paused — manual control active</span>';
    else hint.textContent='Leave strike blank for ATM. Leave lots blank for auto-sizing (10% of capital).';

    // Open position card
    if(d.position){
      const p = d.position;
      const isM = p.source==='manual';
      document.getElementById('pc').style.display  = 'block';
      document.getElementById('pc').className = 'card '+(isM?'pcard-m':'pcard');
      document.getElementById('npc').style.display = 'none';
      const srcBadge = `<span class="${isM?'src-man':'src-auto'}">${isM?'🖐 MANUAL':'🤖 AUTO'}</span>`;
      document.getElementById('pc-title').innerHTML = '📂 Open Position ' + srcBadge;
      document.getElementById('pg').innerHTML = `
        <div class="metric"><div class="mlbl">Symbol</div><div class="mval a">${p.symbol}</div></div>
        <div class="metric"><div class="mlbl">Entry premium</div><div class="mval">₹${fi(p.entry_price)}</div></div>
        <div class="metric"><div class="mlbl">Size</div><div class="mval">${p.lots} lots (${p.qty} units)</div></div>
        <div class="metric"><div class="mlbl">Stop Loss (30%)</div><div class="mval r">₹${fi(d.sl_price)}</div></div>
        <div class="metric"><div class="mlbl">Target (55%)</div><div class="mval g">₹${fi(d.tgt_price)}</div></div>
        <div class="metric"><div class="mlbl">Entry Nifty / Time</div><div class="mval" style="font-size:13px">₹${fi(p.entry_nifty)} · ${p.entry_time}</div></div>
      `;
    } else {
      document.getElementById('pc').style.display  = 'none';
      document.getElementById('npc').style.display = 'block';
    }

    // Trade history
    if(d.trades.length>0){
      document.getElementById('te').style.display = 'none';
      document.getElementById('tt').style.display = 'table';
      document.getElementById('tb').innerHTML = d.trades.map(t=>`
        <tr>
          <td class="a">${t.symbol}</td>
          <td>${t.lots||1}</td>
          <td class="${t.pnl>=0?'g':'r'}">${t.pnl>=0?'+':''}₹${fi0(Math.abs(t.pnl))}</td>
          <td><span class="${t.source==='manual'?'src-man':'src-auto'}">${t.source==='manual'?'🖐':'🤖'}</span></td>
          <td style="color:var(--muted);font-size:10px">${t.reason}</td>
          <td style="color:var(--muted);font-size:10px">${t.entry_time}</td>
        </tr>`).join('');
    }

    // Log
    document.getElementById('lc').innerHTML = d.log.map(l=>`
      <div class="log-item l${l.level}">
        <span class="lt">${l.time}</span><span class="lm">${l.msg}</span>
      </div>`).join('');

  } catch(e){ console.error(e); }
}

tick();
setInterval(tick, 2000);
</script>
</body>
</html>"""

@app.route("/")
def home():
    return render_template_string(HTML)

# ═══════════════════════════════════════
#   MAIN
# ═══════════════════════════════════════
if __name__ == "__main__":
    threading.Thread(target=ltp_monitor, daemon=True).start()
    threading.Thread(target=bot_loop,    daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"NSEAlgo v5 starting on port {port}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
