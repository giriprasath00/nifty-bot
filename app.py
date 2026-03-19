
from flask import Flask, jsonify, render_template_string
from SmartApi import SmartConnect
import pyotp, pandas as pd, threading, time, os, base64, hashlib
from datetime import datetime, timedelta

app = Flask(__name__)

API_KEY     = os.environ.get("API_KEY",     "your_api_key")
CLIENT_CODE = os.environ.get("CLIENT_CODE", "your_client_code")
PASSWORD    = os.environ.get("PASSWORD",    "your_mpin")
TOTP_TOKEN  = os.environ.get("TOTP_TOKEN",  "your_totp_secret")

CAPITAL       = 1000000
LOT_SIZE      = 25
STRIKE_OFFSET = 100
NIFTY_TOKEN   = "99926000"
NIFTY_EXCH    = "NSE"

state = {
    "connected"   : False,
    "nifty_price" : 0,
    "signal"      : "WAITING",
    "reason"      : "Connecting to Angel One...",
    "balance"     : CAPITAL,
    "total_pnl"   : 0,
    "wins"        : 0,
    "losses"      : 0,
    "position"    : None,
    "trades"      : [],
    "last_update" : "—",
    "strategy"    : {"ema": "—", "sr": "—", "volume": "—", "buy_votes": 0, "sell_votes": 0},
    "log"         : [],
}

def add_log(msg, type="info"):
    state["log"].insert(0, {"msg": msg, "type": type, "time": datetime.now().strftime("%H:%M:%S")})
    state["log"] = state["log"][:50]

def get_totp_code(secret):
    # Method 1: direct
    try:
        return pyotp.TOTP(secret).now()
    except Exception:
        pass
    # Method 2: uppercase + strip
    try:
        return pyotp.TOTP(secret.upper().strip()).now()
    except Exception:
        pass
    # Method 3: pad to multiple of 8
    try:
        padded = secret + "=" * (-len(secret) % 8)
        return pyotp.TOTP(padded).now()
    except Exception:
        pass
    # Method 4: base64 decode then base32 encode
    try:
        decoded = base64.b64decode(secret + "==")
        b32 = base64.b32encode(decoded).decode()
        return pyotp.TOTP(b32).now()
    except Exception:
        pass
    # Method 5: encode string as bytes then base32
    try:
        b32 = base64.b32encode(secret.encode()).decode()
        return pyotp.TOTP(b32).now()
    except Exception as e:
        raise Exception(f"All TOTP methods failed: {e}")

def connect_angel():
    try:
        obj  = SmartConnect(api_key=API_KEY)
        totp = get_totp_code(TOTP_TOKEN)
        print(f"TOTP: {totp} | CLIENT: {CLIENT_CODE}")
        data = obj.generateSession(CLIENT_CODE, PASSWORD, totp)
        print(f"Response: {data}")
        if not data["status"]:
            add_log(f"Login failed: {data['message']}", "error")
            return None
        state["connected"] = True
        add_log("Connected to Angel One SmartAPI ✓", "success")
        return obj
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        add_log(f"Connection error: {e}", "error")
        return None

def get_candle_data(obj):
    try:
        now = datetime.now()
        params = {
            "exchange"    : NIFTY_EXCH,
            "symboltoken" : NIFTY_TOKEN,
            "interval"    : "FIVE_MINUTE",
            "fromdate"    : (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M"),
            "todate"      : now.strftime("%Y-%m-%d %H:%M"),
        }
        data = obj.getCandleData(params)
        if not data["status"]:
            add_log(f"Data error: {data['message']}", "error")
            return None
        df = pd.DataFrame(data["data"], columns=["timestamp","open","high","low","close","volume"])
        for col in ["close","volume","high","low"]:
            df[col] = df[col].astype(float)
        return df
    except Exception as e:
        add_log(f"Candle error: {e}", "error")
        return None

def calc_ema(df):
    df["ema9"]  = df["close"].ewm(span=9,  adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
    last, prev  = df.iloc[-1], df.iloc[-2]
    if prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]:
        return "BUY",  "EMA9 crossed above EMA21 ↑"
    if prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]:
        return "SELL", "EMA9 crossed below EMA21 ↓"
    if last["ema9"] > last["ema21"]:
        return "BUY_TREND",  f"EMA9({last['ema9']:.0f}) > EMA21({last['ema21']:.0f})"
    return "SELL_TREND", f"EMA9({last['ema9']:.0f}) < EMA21({last['ema21']:.0f})"

def calc_sr(df):
    recent     = df.tail(20)
    resistance = recent["high"].max()
    support    = recent["low"].min()
    price      = df["close"].iloc[-1]
    prev_price = df["close"].iloc[-2]
    buffer     = (resistance - support) * 0.02
    if prev_price < resistance and price >= resistance - buffer:
        return "SELL", f"Near Resistance ₹{resistance:.0f}"
    if prev_price > support and price <= support + buffer:
        return "BUY",  f"Near Support ₹{support:.0f}"
    return "NEUTRAL", f"S:₹{support:.0f}  R:₹{resistance:.0f}"

def calc_volume(df):
    df["vol_avg"] = df["volume"].rolling(10).mean()
    last, prev    = df.iloc[-1], df.iloc[-2]
    high_vol      = last["volume"] > last["vol_avg"] * 1.5
    ratio         = last["volume"] / last["vol_avg"] if last["vol_avg"] > 0 else 1
    if high_vol and last["close"] > prev["close"]:
        return "BUY",  f"Volume surge ↑ ({ratio:.1f}x avg)"
    if high_vol and last["close"] < prev["close"]:
        return "SELL", f"Volume surge ↓ ({ratio:.1f}x avg)"
    return "NEUTRAL", f"Normal volume ({ratio:.1f}x avg)"

def get_signal(df):
    if df is None or len(df) < 25:
        return "HOLD", "Not enough data", {}
    ema_s, ema_r = calc_ema(df)
    sr_s,  sr_r  = calc_sr(df)
    vol_s, vol_r = calc_volume(df)
    buy_v  = sum(1 for s in [ema_s, sr_s, vol_s] if "BUY"  in s)
    sell_v = sum(1 for s in [ema_s, sr_s, vol_s] if "SELL" in s)
    details = {
        "ema": f"{ema_s} — {ema_r}", "sr": f"{sr_s} — {sr_r}",
        "volume": f"{vol_s} — {vol_r}", "buy_votes": buy_v,
        "sell_votes": sell_v, "price": float(df["close"].iloc[-1]),
    }
    if buy_v  >= 2: return "BUY",  f"{buy_v}/3 strategies say BUY",  details
    if sell_v >= 2: return "SELL", f"{sell_v}/3 strategies say SELL", details
    return "HOLD", "No clear consensus", details

def get_option_price(nifty, otype, strike):
    intrinsic = max(0, nifty - strike) if otype == "CE" else max(0, strike - nifty)
    return round(intrinsic + 75, 2)

def open_trade(signal, price, reason):
    otype  = "CE" if signal == "BUY" else "PE"
    strike = round(price / 100) * 100 + (STRIKE_OFFSET if signal == "BUY" else -STRIKE_OFFSET)
    oprice = get_option_price(price, otype, strike)
    cost   = oprice * LOT_SIZE
    if cost > state["balance"]:
        add_log("Insufficient balance!", "error")
        return
    state["balance"] -= cost
    state["position"] = {
        "type": otype, "strike": strike, "entry_price": oprice,
        "entry_nifty": price, "cost": cost, "reason": reason,
        "entry_time": datetime.now().strftime("%H:%M:%S"),
    }
    add_log(f"{'BUY' if signal=='BUY' else 'SELL'} NIFTY {strike} {otype} @ ₹{oprice} | Cost ₹{cost:,.0f}", "buy" if signal=="BUY" else "sell")

def close_trade(price, reason):
    pos   = state["position"]
    exitp = get_option_price(price, pos["type"], pos["strike"])
    pnl   = (exitp * LOT_SIZE) - pos["cost"]
    state["balance"]   += exitp * LOT_SIZE
    state["total_pnl"] += pnl
    state["position"]   = None
    if pnl >= 0: state["wins"]   += 1
    else:        state["losses"] += 1
    state["trades"].insert(0, {
        "option": f"NIFTY {pos['strike']} {pos['type']}",
        "entry": pos["entry_price"], "exit": exitp, "pnl": round(pnl, 2),
        "entry_time": pos["entry_time"], "exit_time": datetime.now().strftime("%H:%M:%S"),
    })
    add_log(f"EXIT NIFTY {pos['strike']} {pos['type']} @ ₹{exitp} | P&L {'+' if pnl>=0 else ''}₹{pnl:,.0f}", "profit" if pnl>=0 else "loss")

def bot_loop():
    obj = None
    while obj is None:
        obj = connect_angel()
        if obj is None:
            time.sleep(30)
    add_log("Bot started — watching NIFTY every 5 mins", "info")
    while True:
        now          = datetime.now()
        market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=20, second=0, microsecond=0)
        if not (market_open <= now <= market_close):
            if state["position"] and now > market_close:
                df = get_candle_data(obj)
                if df is not None:
                    close_trade(float(df["close"].iloc[-1]), "Market Close")
            state["signal"]      = "MARKET CLOSED"
            state["reason"]      = "Opens Mon–Fri 9:15 AM IST"
            state["last_update"] = now.strftime("%H:%M:%S")
            time.sleep(60)
            continue
        df = get_candle_data(obj)
        signal, reason, details = get_signal(df)
        price = details.get("price", 0)
        if price > 0:
            state.update({"nifty_price": price, "signal": signal, "reason": reason,
                          "strategy": details, "last_update": now.strftime("%H:%M:%S")})
            pos = state["position"]
            if pos is None:
                if signal in ("BUY", "SELL"):
                    open_trade(signal, price, reason)
            else:
                if (pos["type"] == "CE" and signal == "SELL") or (pos["type"] == "PE" and signal == "BUY"):
                    close_trade(price, reason)
        time.sleep(300)

@app.route("/api/state")
def api_state():
    pos     = state["position"]
    unreal  = 0
    if pos:
        cp     = get_option_price(state["nifty_price"], pos["type"], pos["strike"])
        unreal = round((cp - pos["entry_price"]) * LOT_SIZE, 2)
    total   = state["wins"] + state["losses"]
    return jsonify({
        "connected": state["connected"], "nifty_price": state["nifty_price"],
        "signal": state["signal"], "reason": state["reason"],
        "balance": round(state["balance"], 2), "total_pnl": round(state["total_pnl"], 2),
        "pnl_pct": round(state["total_pnl"] / CAPITAL * 100, 2),
        "wins": state["wins"], "losses": state["losses"],
        "winrate": round(state["wins"] / total * 100, 1) if total > 0 else 0,
        "position": pos, "unrealized": unreal,
        "trades": state["trades"][:20], "log": state["log"][:20],
        "last_update": state["last_update"], "strategy": state["strategy"], "capital": CAPITAL,
    })

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NIFTY Paper Bot</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Sora:wght@600;700&display=swap" rel="stylesheet">
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#080c14;color:#94a3b8;font-family:'JetBrains Mono',monospace;min-height:100vh}
  .header{background:#0d1220;border-bottom:1px solid #1e2840;padding:12px 20px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:10}
  .logo{font-family:'Sora',sans-serif;font-size:16px;font-weight:700;color:#fff}.logo span{color:#f97316}
  .live-dot{width:7px;height:7px;border-radius:50%;background:#22c55e;display:inline-block;animation:pulse 1.2s infinite;margin-right:5px}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  .container{max-width:900px;margin:0 auto;padding:16px}
  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
  .grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}
  .card{background:#0d1220;border:1px solid #1e2840;border-radius:10px;padding:14px}
  .card-title{font-size:9px;color:#334155;letter-spacing:.12em;margin-bottom:8px;text-transform:uppercase}
  .big-num{font-family:'Sora',sans-serif;font-size:26px;font-weight:700;color:#fff;letter-spacing:-.02em}
  .small-num{font-size:13px;font-weight:600;color:#fff}
  .green{color:#22c55e}.red{color:#f43f5e}.orange{color:#f97316}.yellow{color:#fbbf24}
  .signal-badge{display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border-radius:6px;font-size:12px;font-weight:700;letter-spacing:.06em}
  .sig-buy{background:#22c55e15;color:#22c55e;border:1px solid #22c55e30}
  .sig-sell{background:#f43f5e15;color:#f43f5e;border:1px solid #f43f5e30}
  .sig-hold{background:#ffffff08;color:#64748b;border:1px solid #ffffff15}
  .sig-closed{background:#fbbf2415;color:#fbbf24;border:1px solid #fbbf2430}
  .strategy-row{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #1e284060;font-size:11px}
  .strategy-row:last-child{border-bottom:none}
  .vote-bar{display:flex;gap:6px;margin-top:8px}
  .vote{flex:1;padding:6px;border-radius:5px;text-align:center;font-size:10px;font-weight:600}
  .vote-buy{background:#22c55e15;color:#22c55e;border:1px solid #22c55e20}
  .vote-sell{background:#f43f5e15;color:#f43f5e;border:1px solid #f43f5e20}
  table{width:100%;border-collapse:collapse;font-size:11px}
  th{color:#334155;font-size:9px;letter-spacing:.1em;padding:6px 8px;text-align:left;border-bottom:1px solid #1e2840}
  td{padding:7px 8px;border-bottom:1px solid #1e284040;color:#94a3b8}
  .log-row{padding:5px 8px;border-radius:4px;font-size:10px;margin-bottom:3px;border-left:2px solid #1e2840}
  .log-buy{border-left-color:#22c55e;color:#22c55e}
  .log-sell{border-left-color:#f43f5e;color:#f43f5e}
  .log-profit{border-left-color:#22c55e80;color:#22c55e}
  .log-loss{border-left-color:#f43f5e80;color:#f43f5e}
  .log-success{border-left-color:#22c55e;color:#22c55e}
  .log-error{border-left-color:#f43f5e;color:#f43f5e}
  .log-info{border-left-color:#1e2840;color:#475569}
  .pos-card{border:1px solid #f9731630;background:#f9731608}
  .stat-card .big-num{font-size:18px}
  .update-badge{font-size:9px;color:#334155;padding:3px 8px;background:#1e284050;border-radius:99px}
  @media(max-width:600px){.grid4{grid-template-columns:repeat(2,1fr)}.grid2{grid-template-columns:1fr}.big-num{font-size:20px}}
</style>
</head>
<body>
<div class="header">
  <div class="logo"><span>NIFTY</span>Bot <span style="font-size:10px;font-weight:400;color:#334155;margin-left:8px">PAPER TRADING</span></div>
  <div style="display:flex;align-items:center;gap:10px">
    <span id="live-indicator" style="font-size:10px;color:#334155">Connecting...</span>
    <span class="update-badge" id="last-update">—</span>
  </div>
</div>
<div class="container">
  <div class="grid2" style="margin-bottom:12px">
    <div class="card">
      <div class="card-title">NIFTY 50 Live Price</div>
      <div class="big-num" id="nifty-price">₹—</div>
      <div style="margin-top:8px;font-size:11px;color:#334155" id="price-sub">Waiting for data...</div>
    </div>
    <div class="card">
      <div class="card-title">Current Signal</div>
      <div id="signal-badge"><span class="signal-badge sig-hold">— WAITING</span></div>
      <div style="margin-top:8px;font-size:11px;color:#475569" id="signal-reason">—</div>
    </div>
  </div>
  <div class="grid4">
    <div class="card stat-card"><div class="card-title">Balance</div><div class="big-num" id="balance">₹—</div></div>
    <div class="card stat-card"><div class="card-title">Total P&L</div><div class="big-num" id="total-pnl">₹0</div><div style="font-size:10px;margin-top:3px" id="pnl-pct">0.00%</div></div>
    <div class="card stat-card"><div class="card-title">Win Rate</div><div class="big-num yellow" id="winrate">0%</div><div style="font-size:10px;color:#334155;margin-top:3px" id="trade-count">0 trades</div></div>
    <div class="card stat-card"><div class="card-title">Wins / Losses</div><div style="display:flex;gap:10px;align-items:baseline;margin-top:4px"><span class="big-num green" id="wins">0</span><span style="color:#334155">/</span><span class="big-num red" id="losses">0</span></div></div>
  </div>
  <div class="card pos-card" id="position-card" style="margin-bottom:12px;display:none">
    <div class="card-title">📂 Open Position</div>
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px" id="position-grid"></div>
  </div>
  <div class="card" id="no-position-card" style="margin-bottom:12px;text-align:center;padding:16px">
    <div style="font-size:11px;color:#334155">📂 No open position — waiting for signal</div>
  </div>
  <div class="card" style="margin-bottom:12px">
    <div class="card-title">Strategy Breakdown (2/3 vote needed)</div>
    <div class="strategy-row"><span style="color:#f97316;font-size:11px">EMA 9/21</span><span id="ema-signal" style="font-size:11px">—</span></div>
    <div class="strategy-row"><span style="color:#818cf8;font-size:11px">Support / Resistance</span><span id="sr-signal" style="font-size:11px">—</span></div>
    <div class="strategy-row"><span style="color:#22c55e;font-size:11px">Volume Momentum</span><span id="vol-signal" style="font-size:11px">—</span></div>
    <div class="vote-bar">
      <div class="vote vote-buy" id="buy-votes">🟢 BUY 0/3</div>
      <div class="vote vote-sell" id="sell-votes">🔴 SELL 0/3</div>
    </div>
  </div>
  <div class="grid2">
    <div class="card">
      <div class="card-title">Trade History</div>
      <div id="trades-empty" style="font-size:11px;color:#334155;padding:10px 0">No trades yet today</div>
      <table id="trades-table" style="display:none">
        <thead><tr><th>Option</th><th>P&L</th><th>Time</th></tr></thead>
        <tbody id="trades-body"></tbody>
      </table>
    </div>
    <div class="card">
      <div class="card-title">Activity Log</div>
      <div id="log-container"></div>
    </div>
  </div>
</div>
<script>
function fmt(n){return new Intl.NumberFormat('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2}).format(n)}
async function update(){
  try{
    const r=await fetch('/api/state');const d=await r.json();
    document.getElementById('live-indicator').innerHTML=d.connected?'<span class="live-dot"></span>Live':'Connecting...';
    document.getElementById('last-update').textContent='Updated '+d.last_update;
    document.getElementById('nifty-price').textContent='₹'+fmt(d.nifty_price);
    document.getElementById('price-sub').textContent='Capital: ₹'+fmt(d.capital);
    const sig=d.signal;let cls='sig-hold',icon='—';
    if(sig==='BUY'){cls='sig-buy';icon='▲'}else if(sig==='SELL'){cls='sig-sell';icon='▼'}else if(sig==='MARKET CLOSED'){cls='sig-closed';icon='💤'}
    document.getElementById('signal-badge').innerHTML=`<span class="signal-badge ${cls}">${icon} ${sig}</span>`;
    document.getElementById('signal-reason').textContent=d.reason;
    const pnlEl=document.getElementById('total-pnl');
    pnlEl.textContent=(d.total_pnl>=0?'+':'')+'₹'+fmt(Math.abs(d.total_pnl));
    pnlEl.className='big-num '+(d.total_pnl>=0?'green':'red');
    document.getElementById('pnl-pct').textContent=(d.pnl_pct>=0?'+':'')+d.pnl_pct+'%';
    document.getElementById('balance').textContent='₹'+fmt(d.balance);
    document.getElementById('winrate').textContent=d.winrate+'%';
    document.getElementById('trade-count').textContent=(d.wins+d.losses)+' trades';
    document.getElementById('wins').textContent=d.wins;
    document.getElementById('losses').textContent=d.losses;
    if(d.position){
      document.getElementById('position-card').style.display='block';
      document.getElementById('no-position-card').style.display='none';
      const p=d.position;const uc=d.unrealized>=0?'green':'red';
      document.getElementById('position-grid').innerHTML=`
        <div><div style="font-size:9px;color:#334155;margin-bottom:3px">OPTION</div><div class="small-num orange">NIFTY ${p.strike} ${p.type}</div></div>
        <div><div style="font-size:9px;color:#334155;margin-bottom:3px">ENTRY</div><div class="small-num">₹${fmt(p.entry_price)}</div></div>
        <div><div style="font-size:9px;color:#334155;margin-bottom:3px">UNREALIZED P&L</div><div class="small-num ${uc}">${d.unrealized>=0?'+':''}₹${fmt(Math.abs(d.unrealized))}</div></div>
        <div><div style="font-size:9px;color:#334155;margin-bottom:3px">NIFTY AT ENTRY</div><div class="small-num">₹${fmt(p.entry_nifty)}</div></div>
        <div><div style="font-size:9px;color:#334155;margin-bottom:3px">COST</div><div class="small-num">₹${fmt(p.cost)}</div></div>
        <div><div style="font-size:9px;color:#334155;margin-bottom:3px">ENTRY TIME</div><div class="small-num">${p.entry_time}</div></div>`;
    }else{
      document.getElementById('position-card').style.display='none';
      document.getElementById('no-position-card').style.display='block';
    }
    const st=d.strategy;
    document.getElementById('ema-signal').textContent=st.ema||'—';
    document.getElementById('sr-signal').textContent=st.sr||'—';
    document.getElementById('vol-signal').textContent=st.volume||'—';
    document.getElementById('buy-votes').textContent='🟢 BUY '+st.buy_votes+'/3';
    document.getElementById('sell-votes').textContent='🔴 SELL '+st.sell_votes+'/3';
    if(d.trades.length>0){
      document.getElementById('trades-empty').style.display='none';
      document.getElementById('trades-table').style.display='table';
      document.getElementById('trades-body').innerHTML=d.trades.map(t=>`<tr><td>${t.option}</td><td class="${t.pnl>=0?'green':'red'}">${t.pnl>=0?'+':''}₹${fmt(Math.abs(t.pnl))}</td><td style="color:#334155">${t.entry_time}</td></tr>`).join('');
    }
    document.getElementById('log-container').innerHTML=d.log.map(l=>`<div class="log-row log-${l.type}"><span style="color:#334155">${l.time} </span>${l.msg}</div>`).join('');
  }catch(e){console.error(e)}
}
update();setInterval(update,10000);
</script>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(HTML)

threading.Thread(target=bot_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
