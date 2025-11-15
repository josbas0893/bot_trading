# ============================================
# Bot de Señales – Render 24/7
# Multi-timeframe + mensaje estilo Josué
# ============================================

import os, re, time, math, asyncio, threading, smtplib, ssl, io, json
from datetime import datetime, timezone, timedelta

import ccxt
import numpy as np
import pandas as pd
import httpx
from email.message import EmailMessage

from fastapi import FastAPI, Request
import uvicorn
from dotenv import load_dotenv

# =========================
# ENV / Secrets
# =========================
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

SMTP_EMAIL        = os.getenv("SMTP_EMAIL", "")
SMTP_APP_PASSWORD = os.getenv("SMTP_APP_PASSWORD", "")
SMTP_TO           = os.getenv("SMTP_TO", SMTP_EMAIL)

PROJECT_NAME = os.getenv("PROJECT_NAME", "BotSenalesRender")

EX_NAMES       = [s.strip() for s in os.getenv("EXCHANGES", "okx,kucoin").split(",")]
PAIRS_ENV      = os.getenv("PAIRS", "").strip()
MAX_PAIRS_ENV  = int(os.getenv("MAX_PAIRS", "150"))
SHIT_FILTER    = os.getenv("SHIT_FILTER", "false").lower() == "true"

RUN_EVERY_SEC  = int(os.getenv("RUN_EVERY_SEC", "300"))  # escaneo cada 5 min
TIMEZONE_BOT   = os.getenv("BOT_TZ", "America/Mexico_City")

# =========================
# Modos
# =========================
MODES = {
    "suave": {
        "name": "Suave",
        "pb_min": 0.35,   # % pullback mínimo (0–1) sobre rango
        "pb_max": 0.80,
        "min_score": 55,
        "rr_min": 1.3,
        "atr_mult_sl": 1.3,
        "atr_mult_tp1": 1.8,
        "atr_mult_tp2": 2.5,
        "atr_mult_tp3": 3.2,
    },
    "normal": {
        "name": "Normal",
        "pb_min": 0.45,
        "pb_max": 0.75,
        "min_score": 65,
        "rr_min": 1.6,
        "atr_mult_sl": 1.5,
        "atr_mult_tp1": 2.0,
        "atr_mult_tp2": 3.0,
        "atr_mult_tp3": 4.0,
    },
    "sniper": {
        "name": "Sniper",
        "pb_min": 0.52,
        "pb_max": 0.70,
        "min_score": 75,
        "rr_min": 1.9,
        "atr_mult_sl": 1.7,
        "atr_mult_tp1": 2.3,
        "atr_mult_tp2": 3.5,
        "atr_mult_tp3": 5.0,
    },
}

MODE = {"current": "normal"}   # arranca en modo normal

STATE = {
    "started": False,
    "last_sent": {},   # (symbol, side) -> timestamp
}

MONITOR_ACTIVE = True

# =========================
# Exchanges (ccxt)
# =========================
EX_OBJS = {}

def init_exchanges():
    for name in EX_NAMES:
        try:
            klass = getattr(ccxt, name)
            opts = {"enableRateLimit": True}
            ex = klass(opts)
            ex.load_markets()
            EX_OBJS[name] = ex
            print(f"✅ Conectado a {name} con {len(ex.symbols)} símbolos.")
        except Exception as e:
            print(f"⚠️ No se pudo iniciar {name}: {e}")

def _basic_usdt_filter(symbol: str) -> bool:
    if not symbol.endswith("/USDT"):
        return False
    if "PERP" in symbol.upper():
        return False
    return True

def _collect_from(ex):
    try:
        return [s for s in ex.symbols if _basic_usdt_filter(s)]
    except:
        return []

def build_pairs_dynamic(limit=150):
    agg, seen = [], set()
    for name in EX_NAMES:
        ex = EX_OBJS.get(name)
        if not ex:
            continue
        syms = _collect_from(ex)
        for s in syms:
            if s not in seen:
                agg.append(s)
                seen.add(s)
        if len(agg) >= limit:
            break
    return sorted(agg)[:limit]

init_exchanges()

# ========= PARES =========
USER_PAIRS = []
if PAIRS_ENV:
    USER_PAIRS = [s.strip() for s in PAIRS_ENV.split(",") if s.strip()]

if USER_PAIRS:
    ACTIVE_PAIRS = USER_PAIRS[:MAX_PAIRS_ENV]
else:
    ACTIVE_PAIRS = build_pairs_dynamic(limit=MAX_PAIRS_ENV)

print(f"📦 Pares a monitorear: {len(ACTIVE_PAIRS)} (manual={'sí' if USER_PAIRS else 'no'})")

# =========================
# Telegram (simple via HTTP)
# =========================
async def send_tg(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(url, data=data)
            if r.status_code != 200:
                print("❌ Error Telegram:", r.status_code, r.text)
    except Exception as e:
        print("❌ Telegram error:", e)

LAST_UPDATE_ID = 0

async def poll_telegram_commands():
    """
    Loop sencillo para leer comandos desde el mismo chat (sin webhooks).
    Soporta: /start, /stop, /status, /mode suave|normal|sniper
    """
    global LAST_UPDATE_ID, MONITOR_ACTIVE
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {
        "timeout": 0,
        "offset": LAST_UPDATE_ID + 1,
    }
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.get(url, params=params)
            data = r.json()
    except Exception as e:
        print("poll_telegram_commands error:", e)
        return

    for upd in data.get("result", []):
        LAST_UPDATE_ID = upd["update_id"]
        msg = upd.get("message") or upd.get("edited_message")
        if not msg:
            continue
        chat_id = str(msg["chat"]["id"])
        if chat_id != TELEGRAM_CHAT_ID:
            continue
        text = (msg.get("text") or "").strip()
        if not text.startswith("/"):
            continue

        parts = text.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd == "/start":
            MONITOR_ACTIVE = True
            await send_tg("✅ Bot ACTIVADO (monitor_loop).")
        elif cmd == "/stop":
            MONITOR_ACTIVE = False
            await send_tg("🛑 Bot DETENIDO (monitor_loop).")
        elif cmd == "/status":
            await send_tg(
                f"📊 <b>ESTADO</b>\n"
                f"• Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
                f"• Modo: <b>{MODE['current'].upper()}</b>\n"
                f"• Monitoreo: <b>{'ACTIVO' if MONITOR_ACTIVE else 'DETENIDO'}</b>"
            )
        elif cmd == "/mode":
            if args and args[0].lower() in MODES:
                MODE["current"] = args[0].lower()
                await send_tg(f"⚙️ Modo cambiado a <b>{MODE['current'].upper()}</b>.")
            else:
                await send_tg("Usa: <code>/mode suave</code>, <code>/mode normal</code> o <code>/mode sniper</code>.")

# =========================
# Helpers generales
# =========================
def local_time_str():
    # Para que se vea similar a CST / GMT-5
    tz = timezone(timedelta(hours=-5))
    return datetime.now(tz).strftime("%d %b %H:%M")

def can_send(pair, direction):
    """anti-spam: no repetir mismo par/dirección en 30 min"""
    key = (pair, direction)
    t = STATE["last_sent"].get(key, 0)
    return (time.time() - t) > (30 * 60)

def mark_sent(pair, direction):
    STATE["last_sent"][(pair, direction)] = time.time()

def fmt_price(x):
    try:
        if x < 0.0001:
            return f"{x:.8f}"
        if x < 1:
            return f"{x:.6f}"
        if x < 100:
            return f"{x:.4f}"
        return f"{x:.2f}"
    except Exception:
        return str(x)

# =========================
# Filtro de "shitcoins"
# =========================
SHIT_RE = re.compile(
    r"(PEPE|SHIB|FLOKI|DOGE|BONK|BABY|INU|MEME|FART|PUMP|MOON|RATS|CAT|CHEEMS|TURBO)",
    re.IGNORECASE,
)

def is_shitcoin(symbol: str) -> bool:
    """
    Heurística simple: mira el "base" del par (antes del /USDT).
    Si no quieres filtrar nada, pon SHIT_FILTER=false en env.
    """
    base = symbol.split("/")[0]
    if SHIT_RE.search(base):
        return True
    # Tokens tipo 1000XXXXX, 1MXXXXX, etc
    if re.match(r"^(1000|1M|1MB|10K)", base, re.IGNORECASE):
        return True
    return False

# =========================
# OHLCV + indicadores
# =========================
def fetch_ohlcv_first_ok(symbol, timeframe, limit=300):
    for _, ex in EX_OBJS.items():
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if data and len(data) > 50:
                df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "volume"])
                df = df.iloc[:-1].copy()  # quitar vela en formación
                df["ts"] = pd.to_datetime(df["ts"], unit="ms")
                df.set_index("ts", inplace=True)
                return df
        except Exception:
            continue
    return pd.DataFrame()

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    roll_up = gain.ewm(alpha=1/length, adjust=False).mean()
    roll_down = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val.fillna(50)

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["EMA_12"]  = ema(df["close"], 12)
    df["EMA_20"]  = ema(df["close"], 20)
    df["EMA_50"]  = ema(df["close"], 50)
    df["EMA_100"] = ema(df["close"], 100)
    df["EMA_200"] = ema(df["close"], 200)
    df["RSI"]     = rsi(df["close"], 14)

    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"]  - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    return df.dropna()

def premium_discount_zone(price, low, high):
    """
    Clasifica la zona: discount / premium / eq (equilibrio).
    """
    if high <= low:
        return "EQ"
    rango = high - low
    if rango <= 0:
        return "EQ"
    # 0–1 => desde el low a high
    pos = (price - low) / rango
    if pos < 0.35:
        return "DEEP_DISCOUNT"
    if pos < 0.4:
        return "DISCOUNT"
    if pos > 0.65:
        return "DEEP_PREMIUM"
    if pos > 0.6:
        return "PREMIUM"
    return "EQ"

# =========================
# Análisis multi-timeframe
# =========================
def trend_from_4h(df4: pd.DataFrame):
    """
    Swing (4h) – usa EMA100 para bias principal.
    """
    last = df4.iloc[-1]
    close = float(last["close"])
    ema100 = float(last["EMA_100"])
    if close > ema100 * 1.003:
        return 1  # long
    if close < ema100 * 0.997:
        return -1  # short
    return 0

def bias_from_ema_cluster(df: pd.DataFrame):
    """
    Macro (1h) y Confirm (30m):
    - long: close>EMA200 y EMA12>EMA50>EMA200
    - short: close<EMA200 y EMA12<EMA50<EMA200
    """
    last = df.iloc[-1]
    c = float(last["close"])
    e12 = float(last["EMA_12"])
    e50 = float(last["EMA_50"])
    e200 = float(last["EMA_200"])

    if c > e200 and e12 > e50 > e200:
        return 1
    if c < e200 and e12 < e50 < e200:
        return -1
    return 0

def pullback_ratio(price, low, high, is_long: bool):
    """
    Retorna un valor 0–1 de cuánto ha retrocedido el precio en el rango.
    Para long: 0 = high, 1 = low (retroceso profundo).
    Para short: 0 = low,  1 = high.
    """
    if high <= low:
        return 0.0
    if is_long:
        return (high - price) / (high - low)
    else:
        return (price - low) / (high - low)

def simple_5m_confirmation(df5: pd.DataFrame, side: str):
    """
    Confirmación sencilla en 5m:
    - close vs open (candle en dirección)
    - EMA20 vs EMA50
    """
    if len(df5) < 3:
        return False
    last = df5.iloc[-1]
    o = float(last["open"])
    c = float(last["close"])
    e20 = float(last["EMA_20"])
    e50 = float(last["EMA_50"])

    if side == "LONG":
        return (c > o) and (c > e20) and (e20 > e50)
    else:
        return (c < o) and (c < e20) and (e20 < e50)

def compute_rr(price, sl, tp):
    risk = abs(price - sl)
    reward = abs(tp - price)
    if risk <= 0:
        return 0.0
    return reward / risk

def analyze_symbol(symbol: str):
    """
    Lógica principal:
    4h → swing
    1h → macro (obligatorio)
    30m → confirm extra
    15m → pullback + RSI
    5m → confirm de entrada
    """
    # shitcoin filter opcional
    if SHIT_FILTER and is_shitcoin(symbol):
        return None

    # 4h / 1h / 30m / 15m / 5m
    df4  = fetch_ohlcv_first_ok(symbol, "4h",  250)
    df1  = fetch_ohlcv_first_ok(symbol, "1h",  250)
    df30 = fetch_ohlcv_first_ok(symbol, "30m", 250)
    df15 = fetch_ohlcv_first_ok(symbol, "15m", 250)
    df5  = fetch_ohlcv_first_ok(symbol, "5m",  250)

    if df4.empty or df1.empty or df30.empty or df15.empty or df5.empty:
        return None

    df4  = compute_indicators(df4)
    df1  = compute_indicators(df1)
    df30 = compute_indicators(df30)
    df15 = compute_indicators(df15)
    df5  = compute_indicators(df5)

    if any(df.empty for df in [df4, df1, df30, df15, df5]):
        return None

    # --- 4h swing bias ---
    swing = trend_from_4h(df4)
    if swing == 0:
        return None

    # --- 1h macro bias (obligatorio) ---
    macro = bias_from_ema_cluster(df1)
    if macro == 0:
        return None
    if macro != swing:
        return None

    # --- 30m confirm bias ---
    confirm30 = bias_from_ema_cluster(df30)

    # side final
    side = "LONG" if macro == 1 else "SHORT"
    is_long = (side == "LONG")

    score = 0
    notes = []

    # Puntos por 4h
    score += 30
    notes.append("4H alineado (EMA100 / bias principal)")

    # 30m
    if confirm30 == macro:
        score += 15
        notes.append("30m alineado con la tendencia")
    else:
        notes.append("30m neutro/ligeramente contrario")

    # --- 15m: pullback + RSI ---
    last15 = df15.iloc[-1]
    c15 = float(last15["close"])
    rsi15 = float(last15["RSI"])

    swing_low_15  = float(df15["low"].tail(80).min())
    swing_high_15 = float(df15["high"].tail(80).max())

    pb = pullback_ratio(c15, swing_low_15, swing_high_15, is_long=is_long)

    mode_cfg = MODES.get(MODE["current"], MODES["normal"])
    pb_min, pb_max = mode_cfg["pb_min"], mode_cfg["pb_max"]

    pb_ok = (pb_min <= pb <= pb_max)

    if pb_ok:
        score += 20
        notes.append(f"Rebote/pullback 15m en zona saludable ({pb:.2f})")
    else:
        notes.append(f"Pullback 15m fuera de zona ({pb:.2f})")

    # RSI 15m
    if is_long and 40 <= rsi15 <= 65:
        score += 10
        notes.append("RSI 15m en rango bueno para LONG")
    elif (not is_long) and 35 <= rsi15 <= 60:
        score += 10
        notes.append("RSI 15m en rango bueno para SHORT")
    else:
        notes.append(f"RSI 15m {rsi15:.1f} no ideal")

    # --- 5m confirmación ---
    conf5 = simple_5m_confirmation(df5, side)
    if conf5:
        score += 15
        notes.append("Confirmación 5m (EMA20/50 + vela en dirección)")
    else:
        notes.append("5m sin confirmación fuerte")

    # --- Contexto de zona (premium/discount/eq/extrema) ---
    zona = premium_discount_zone(c15, swing_low_15, swing_high_15)
    if zona == "DEEP_DISCOUNT" and is_long:
        notes.append("Zona DEEP_DISCOUNT (buena para LONG)")
    elif zona == "DEEP_PREMIUM" and not is_long:
        notes.append("Zona DEEP_PREMIUM (buena para SHORT)")
    else:
        notes.append(f"Zona {zona} 15m")

    # --- SL / TPs calculados con ATR en 15m ---
    atr15 = float(last15["ATR"])
    price_exec = float(df5["close"].iloc[-1])

    atr_sl_mult  = mode_cfg["atr_mult_sl"]
    atr_tp1_mult = mode_cfg["atr_mult_tp1"]
    atr_tp2_mult = mode_cfg["atr_mult_tp2"]
    atr_tp3_mult = mode_cfg["atr_mult_tp3"]

    if is_long:
        sl  = price_exec - atr15 * atr_sl_mult
        tp1 = price_exec + atr15 * atr_tp1_mult
        tp2 = price_exec + atr15 * atr_tp2_mult
        tp3 = price_exec + atr15 * atr_tp3_mult
    else:
        sl  = price_exec + atr15 * atr_sl_mult
        tp1 = price_exec - atr15 * atr_tp1_mult
        tp2 = price_exec - atr15 * atr_tp2_mult
        tp3 = price_exec - atr15 * atr_tp3_mult

    rr1 = compute_rr(price_exec, sl, tp1)
    notes.append(f"RR1 estimado: {rr1:.2f}")

    # mínima calidad por modo
    if score < mode_cfg["min_score"]:
        return None
    if rr1 < mode_cfg["rr_min"]:
        notes.append("RR1 por debajo del mínimo del modo")
        return None

    result = {
        "symbol": symbol,
        "side": side,
        "price": price_exec,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "zona": zona,
        "score": score,
        "mode_name": mode_cfg["name"],
        "notes": notes,
        "exec_tf": "5m",
        "trigger_tf": "15m",   # <--- para el texto "Temporalidad gatillo"
    }
    return result

# =========================
# Mensaje de señal (TU FORMATO)
# =========================
def build_signal_message(d: dict) -> str:
    """
    Formato estilo Josué:

    ✨ SEÑAL DETECTADA ✨

    🟢 LONG 📈
    💱 Par: DAI/USDT
    🕒 Temporalidad gatillo: 15m
    ⚙️ Modo: NORMAL
    📊 Score: 65
    📌 Zona: DEEP_DISCOUNT
    ...
    """
    side   = d["side"]
    symbol = d["symbol"]
    price  = d["price"]
    sl     = d["sl"]
    tp1    = d["tp1"]
    tp2    = d["tp2"]
    tp3    = d["tp3"]
    zona   = d["zona"]
    score  = d["score"]
    mode_name  = d["mode_name"]
    trigger_tf = d.get("trigger_tf", "15m")
    notes  = d["notes"]

    long_short_line = "🟢 LONG 📈" if side == "LONG" else "🔴 SHORT 📉"

    msg = []
    msg.append("✨ SEÑAL DETECTADA ✨")
    msg.append("")
    msg.append(long_short_line)
    msg.append(f"💱 Par: {symbol}")
    msg.append(f"🕒 Temporalidad gatillo: {trigger_tf}")
    msg.append(f"⚙️ Modo: {mode_name.upper()}")
    msg.append(f"📊 Score: {score}")
    msg.append(f"📌 Zona: {zona}")
    msg.append("")
    msg.append(f"🎯 Entrada: {fmt_price(price)}")
    msg.append(f"🛑 SL: {fmt_price(sl)}")
    msg.append(f"✅ TP1: {fmt_price(tp1)}")
    msg.append(f"✅ TP2: {fmt_price(tp2)}")
    msg.append(f"✅ TP3: {fmt_price(tp3)}")
    msg.append("")
    msg.append("🧠 Confirmaciones:")
    for n in notes:
        msg.append(f"• {n}")
    msg.append("")
    msg.append("⚠️ Gestiona riesgo, ajusta tamaño de posición y SL según tu capital.")

    return "\n".join(msg)

# =========================
DAILY_SIGNALS = []

def register_signal(d: dict):
    x = dict(d)
    x["ts"] = datetime.now(timezone.utc).isoformat()
    DAILY_SIGNALS.append(x)

def send_email_with_attachments(subject: str, body: str, attachments: list):
    if not (SMTP_EMAIL and SMTP_APP_PASSWORD and SMTP_TO):
        print("⚠️ SMTP no configurado; no se envía correo.")
        return

    msg = EmailMessage()
    msg["From"] = SMTP_EMAIL
    msg["To"]   = SMTP_TO
    msg["Subject"] = subject
    msg.set_content(body)

    for filename, file_bytes, mime in attachments:
        maintype, subtype = mime.split("/")
        msg.add_attachment(file_bytes, maintype=maintype, subtype=subtype, filename=filename)

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(SMTP_EMAIL, SMTP_APP_PASSWORD)
        s.send_message(msg)

async def email_daily_signals_excel():
    if not DAILY_SIGNALS:
        return
    df = pd.DataFrame(DAILY_SIGNALS)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="signals")
    buf.seek(0)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    send_email_with_attachments(
        f"[{PROJECT_NAME}] Señales {day}",
        f"Adjunto Excel con señales del {day} (UTC).",
        [(f"signals_{day}.xlsx", buf.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
    )
    DAILY_SIGNALS.clear()

# =========================
# Loops principales
# =========================
async def monitor_loop():
    await send_tg(
        f"✅ <b>{PROJECT_NAME}</b> iniciado.\n"
        f"🧭 Modo: <b>{MODE['current'].upper()}</b>\n"
        f"🔎 Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"⏱️ Escaneo cada <b>{RUN_EVERY_SEC//60} min</b>"
    )
    while True:
        if not MONITOR_ACTIVE:
            await asyncio.sleep(3)
            continue

        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Escaneando mercado (modo={MODE['current']})…")
        for sym in ACTIVE_PAIRS:
            try:
                res = analyze_symbol(sym)
                if not res:
                    continue
                side = res["side"]
                if not can_send(sym, side):
                    continue
                msg = build_signal_message(res)
                await send_tg(msg)
                register_signal(res)
                mark_sent(sym, side)
            except Exception as e:
                print(f"⚠️ Error analizando {sym}: {e}")

        await asyncio.sleep(RUN_EVERY_SEC)

async def scheduler_loop():
    last_day = None
    while True:
        # comandos telegram
        try:
            await poll_telegram_commands()
        except Exception as e:
            print("poll_telegram_commands loop error:", e)

        # envío excel diario a las 23:59 UTC aprox
        now = datetime.now(timezone.utc)
        day = now.date()
        if last_day is None:
            last_day = day
        if day != last_day and now.hour >= 0:
            try:
                await email_daily_signals_excel()
            except Exception as e:
                print("email_daily_signals_excel error:", e)
            last_day = day

        await asyncio.sleep(2)

# =========================
# FastAPI keep-alive (/ping)
# =========================
app = FastAPI()

@app.get("/ping")
async def ping():
    return {"ok": True, "service": PROJECT_NAME, "time": datetime.utcnow().isoformat()}

def start_http():
    def _run():
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), log_level="warning")
    th = threading.Thread(target=_run, daemon=True)
    th.start()

# =========================
# Entrypoint
# =========================
async def main_async():
    start_http()
    await asyncio.gather(
        monitor_loop(),
        scheduler_loop(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except RuntimeError:
        loop = asyncio.get_event_loop()
        loop.create_task(main_async())
        loop.run_forever()
