# =========================
# Bot de Señales — Swing 4H / Macro 1H / Confirm 30m / Gatillo 15m / Exec 5m
# Compatible con Replit y Render (24/7 con keep-alive HTTP)
# =========================

import os, re, time, math, asyncio, threading, smtplib, ssl, io
from datetime import datetime, UTC
from email.message import EmailMessage

import ccxt
import numpy as np
import pandas as pd

from fastapi import FastAPI, Request
import uvicorn
from dotenv import load_dotenv

from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# =========================
# ENV / Secrets
# =========================
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# Gmail solo para adjuntar CSV/Excel (opcional)
SMTP_EMAIL        = os.getenv("SMTP_EMAIL", "")
SMTP_APP_PASSWORD = os.getenv("SMTP_APP_PASSWORD", "")
SMTP_TO           = os.getenv("SMTP_TO", SMTP_EMAIL)

PROJECT_NAME = os.getenv("PROJECT_NAME", "BotSenalesSwing")

# EXCHANGES: "okx,kucoin,bybit,binance"
EX_NAMES       = [s.strip() for s in os.getenv("EXCHANGES", "okx,kucoin,bybit,binance").split(",")]
# PAIRS: "auto" para dinámico, o "BTC/USDT,ETH/USDT,SOL/USDT"
PAIRS_ENV      = os.getenv("PAIRS", "auto")
MAX_PAIRS_ENV  = int(os.getenv("MAX_PAIRS", "150"))
TV_SECRET      = os.getenv("TV_SECRET", "")

# =========================
# Parámetros generales
# =========================
RUN_EVERY_SEC = 300   # escaneo cada 5 minutos

# Modos: soft, normal, sniper
MODE = {"current": "normal"}   # arranque en NORMAL
MONITOR_ACTIVE = True

MODE_CONFIG = {
    "soft": {
        "fib_tol": 0.010,   # 1%
        "ema_tol": 0.008,   # 0.8%
        "rsi_long": (35, 65),
        "rsi_short": (35, 65),
        "min_score": 50
    },
    "normal": {
        "fib_tol": 0.006,   # 0.6%
        "ema_tol": 0.005,   # 0.5%
        "rsi_long": (40, 60),
        "rsi_short": (40, 60),
        "min_score": 65
    },
    "sniper": {
        "fib_tol": 0.004,   # 0.4%
        "ema_tol": 0.003,   # 0.3%
        "rsi_long": (42, 58),
        "rsi_short": (42, 58),
        "min_score": 80
    },
}

FIB_PULLBACK_LEVELS = [0.5, 0.618, 0.65, 0.75]
ATR_MULT_SL = 1.8

STATE = {
    "last_sent": {}   # (symbol, side) -> timestamp
}

DAILY_SIGNALS = []    # para Excel diario

# =========================
# Exchanges (ccxt)
# =========================
EX_OBJS = {}

def init_exchanges():
    for name in EX_NAMES:
        try:
            klass = getattr(ccxt, name)
            opts = {"enableRateLimit": True}
            if name in ("bybit", "kucoin"):
                opts["options"] = {"defaultType": "future"}
            ex = klass(opts)
            ex.load_markets()
            EX_OBJS[name] = ex
            print(f"✅ Conectado a {name} con {len(ex.symbols)} símbolos.")
        except Exception as e:
            print(f"⚠️ No se pudo iniciar {name}: {e}")

def _basic_usdt_filter(symbol: str) -> bool:
    if not symbol.endswith("/USDT"):
        return False
    if re.match(r"^(1000|1M|1MB|10K|B-|.*-).*?/USDT$", symbol):
        return False
    if "PERP" in symbol.upper():
        return False
    return True

def _collect_from(ex):
    try:
        return [s for s in ex.symbols if _basic_usdt_filter(s)]
    except Exception:
        return []

def build_pairs_dynamic(limit=150):
    agg, seen = [], set()
    for name in EX_NAMES:
        ex = EX_OBJS.get(name)
        if not ex:
            continue
        for s in _collect_from(ex):
            if s not in seen:
                agg.append(s); seen.add(s)
            if len(agg) >= limit:
                break
        if len(agg) >= limit:
            break
    return sorted(agg)[:limit]

init_exchanges()

USER_PAIRS = []
if PAIRS_ENV and PAIRS_ENV.strip().lower() != "auto":
    USER_PAIRS = [s.strip() for s in PAIRS_ENV.split(",") if s.strip()]

DYN = build_pairs_dynamic(limit=MAX_PAIRS_ENV)
if USER_PAIRS:
    seen = set()
    ACTIVE_PAIRS = []
    for s in USER_PAIRS + DYN:
        if s not in seen:
            ACTIVE_PAIRS.append(s)
            seen.add(s)
    ACTIVE_PAIRS = ACTIVE_PAIRS[:MAX_PAIRS_ENV]
else:
    ACTIVE_PAIRS = DYN

print(f"📦 Pares a monitorear: {len(ACTIVE_PAIRS)} (modo pares={'manual+auto' if USER_PAIRS else 'auto'})")

# =========================
# Telegram helpers
# =========================
async def send_tg(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Falta TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")
        return
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        print("❌ Telegram error:", e)

async def startup_notice():
    await send_tg(
        f"✅ {PROJECT_NAME} iniciado\n"
        f"🧭 Modo: <b>{MODE['current'].upper()}</b>\n"
        f"📦 Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"⏱️ Escaneo cada <b>{RUN_EVERY_SEC//60} min</b>\n"
        f"⛓️ Timeframes: 4H (swing) / 1H (macro) / 30m (confirm) / 15m (gatillo) / 5m (exec)"
    )

def can_send(symbol: str, side: str) -> bool:
    key = (symbol, side)
    t0 = STATE["last_sent"].get(key, 0)
    # 30 minutos mínimo entre señales del mismo par y sentido
    return (time.time() - t0) > 30 * 60

def mark_sent(symbol: str, side: str):
    STATE["last_sent"][(symbol, side)] = time.time()

def fmt_price(x: float) -> str:
    try:
        if x < 1:
            return f"{x:.6f}"
        elif x < 100:
            return f"{x:.4f}"
        else:
            return f"{x:.2f}"
    except Exception:
        return str(x)

def build_signal_message(symbol, side, tf_exec, entry, sl, tp1, tp2, tp3, score, ctx):
    dir_emoji = "🟢 LONG 📈" if side == "LONG" else "🔴 SHORT 📉"
    zona_txt = ctx.get("zona_valor", "EQ")
    fib_txt = ctx.get("fib_txt", "")
    rsi_txt = ctx.get("rsi_txt", "")
    ema_txt = ctx.get("ema_txt", "")
    patt_txt = ctx.get("patt_txt", "")
    trend_txt = ctx.get("trend_txt", "")

    return (
        f"{'🟢' if side=='LONG' else '🔴'} SEÑAL {side} {'🟢' if side=='LONG' else '🔴'}\n\n"
        f"💱 {symbol}\n"
        f"📊 Exec: {tf_exec}\n"
        f"⏱️ Modo: <b>{MODE['current'].upper()}</b>\n"
        f"⭐ Score: <b>{score:.1f}</b>/100\n\n"
        f"➡️ {dir_emoji}\n"
        f"💰 Entrada: <code>{fmt_price(entry)}</code>\n"
        f"🛑 SL: <code>{fmt_price(sl)}</code>\n"
        f"🎯 TP1: <code>{fmt_price(tp1)}</code>\n"
        f"🎯 TP2: <code>{fmt_price(tp2)}</code>\n"
        f"🎯 TP3: <code>{fmt_price(tp3)}</code>\n\n"
        f"📍 Zona: <b>{zona_txt}</b>\n"
        f"📏 {fib_txt}\n"
        f"📉 {rsi_txt}\n"
        f"📐 {ema_txt}\n"
        f"📈 {trend_txt}\n"
        f"🧩 {patt_txt}\n\n"
        f"⚠️ Gestiona riesgo, mueve SL a BE en TP1.\n"
        f"No es consejo financiero, opera bajo tu propio criterio. 🍀"
    )

# =========================
# Indicadores nativos (sin pandas_ta)
# =========================
def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    roll_up = gain.ewm(alpha=1/length, adjust=False).mean()
    roll_down = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
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

def fetch_ohlcv_first_ok(symbol, timeframe, limit=500):
    for _, ex in EX_OBJS.items():
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not data or len(data) < 50:
                continue
            df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
            # quitamos la última vela en curso
            df = df.iloc[:-1].copy()
            df["ts"] = pd.to_datetime(df["ts"], unit="ms")
            df.set_index("ts", inplace=True)
            return compute_indicators(df)
        except Exception:
            continue
    return pd.DataFrame()

# =========================
# Lógica de tendencia, zona y patrones
# =========================
def trend_4h(df4):
    last = df4.iloc[-1]
    price = last["close"]
    ema100 = last["EMA_100"]
    if price > ema100 * 1.002:
        return 1   # swing alcista
    elif price < ema100 * 0.998:
        return -1  # swing bajista
    return 0

def trend_tf_emas(df, bias_hint=None):
    last = df.iloc[-1]
    price = last["close"]
    e12, e50, e200 = last["EMA_12"], last["EMA_50"], last["EMA_200"]

    if price > e50 and e12 > e50 > e200:
        return 1
    if price < e50 and e12 < e50 < e200:
        return -1
    # si no hay tendencia clara, usa hint
    return bias_hint or 0

def detect_range_bias(df, bars=40):
    seg = df.tail(bars)
    high = seg["high"].max()
    low  = seg["low"].min()
    mid  = (high + low) / 2.0
    last_price = seg["close"].iloc[-1]
    first_price = seg["close"].iloc[0]
    if last_price > mid and last_price > first_price * 1.002:
        return 1   # rango pero empujando arriba
    if last_price < mid and last_price < first_price * 0.998:
        return -1  # rango pero empujando abajo
    return 0

def premium_discount(price, low, high):
    if high <= low:
        return "EQ"
    r = high - low
    discount = low + 0.3 * r
    premium  = high - 0.3 * r
    if price < discount:
        return "DISCOUNT"
    if price > premium:
        return "PREMIUM"
    return "EQ"

def fib_pullback_ok(price, low, high, mode_name):
    cfg = MODE_CONFIG[mode_name]
    tol = cfg["fib_tol"]
    if high <= low:
        return False, ""
    range_ = high - low
    levels = {}
    for r in FIB_PULLBACK_LEVELS:
        lvl = high - range_ * r
        levels[r] = lvl
    for r, lvl in levels.items():
        if abs(price - lvl) / max(lvl, 1e-9) <= tol:
            return True, f"Pullback Fib {r:.3f} ({fmt_price(lvl)})"
    return False, "Fuera de zona Fib"

def rsi_hook(df, side, mode_name):
    cfg = MODE_CONFIG[mode_name]
    rsi_lo, rsi_hi = (cfg["rsi_long"] if side == "LONG" else cfg["rsi_short"])
    if len(df) < 5:
        return False, "RSI insuficiente"
    rsi_vals = df["RSI"].iloc[-3:]
    r0, r1, r2 = rsi_vals.iloc[0], rsi_vals.iloc[1], rsi_vals.iloc[2]
    if side == "LONG":
        ok_range = rsi_lo <= r2 <= rsi_hi
        giro = (r2 > r1) and (r1 >= r0)
        return (ok_range and giro), f"RSI giro ↑ ({r2:.1f})"
    else:
        ok_range = rsi_lo <= r2 <= rsi_hi
        giro = (r2 < r1) and (r1 <= r0)
        return (ok_range and giro), f"RSI giro ↓ ({r2:.1f})"

def ema_touch(df, side, mode_name):
    cfg = MODE_CONFIG[mode_name]
    tol = cfg["ema_tol"]
    last = df.iloc[-1]
    price = last["close"]
    ema50 = last["EMA_50"]
    ema200 = last["EMA_200"]
    near_ema50 = abs(price - ema50) / max(price, 1e-9) <= tol
    if side == "LONG":
        cond = (price >= ema50) and (ema50 >= ema200 * 0.995)
        return (cond and near_ema50), f"Hook EMA50↑ ({fmt_price(ema50)})"
    else:
        cond = (price <= ema50) and (ema50 <= ema200 * 1.005)
        return (cond and near_ema50), f"Hook EMA50↓ ({fmt_price(ema50)})"

def micro_confirm_5m(df5, side):
    last = df5.iloc[-1]
    prev = df5.iloc[-2]
    price = last["close"]
    ema20 = last["EMA_20"]
    ema50 = last["EMA_50"]
    if side == "LONG":
        cond1 = price > ema20 > ema50
        cond2 = last["close"] > last["open"] >= prev["close"]
        return cond1 and cond2, "5m a favor (velas verdes)"
    else:
        cond1 = price < ema20 < ema50
        cond2 = last["close"] < last["open"] <= prev["close"]
        return cond1 and cond2, "5m a favor (velas rojas)"

def double_top_bottom(df, bars=40, tol=0.004):
    seg = df.tail(bars)
    hi = seg["high"].values
    lo = seg["low"].values
    max1 = hi.max()
    min1 = lo.min()
    last_hi = seg["high"].tail(bars//2).max()
    last_lo = seg["low"].tail(bars//2).min()
    two_top = abs(last_hi - max1) / max(max1, 1e-9) <= tol
    two_bot = abs(last_lo - min1) / max(min1, 1e-9) <= tol
    return two_top, two_bot

# =========================
# TP / SL usando swing 1H
# =========================
def compute_tp_sl_from_1h(side, entry, df1, df15, df5):
    # swings
    sw1_hi = df1["high"].tail(120).max()
    sw1_lo = df1["low"].tail(120).min()
    sw15_hi = df15["high"].tail(60).max()
    sw15_lo = df15["low"].tail(60).min()
    atr_exec = df5["ATR"].iloc[-1]
    if side == "LONG":
        risk_zone = min(sw15_lo, entry - ATR_MULT_SL * atr_exec)
        sl = float(risk_zone)
        r = sw1_hi - sw1_lo
        tp1 = entry + r * 0.382
        tp2 = entry + r * 0.618
        tp3 = entry + r * 1.0
    else:
        risk_zone = max(sw15_hi, entry + ATR_MULT_SL * atr_exec)
        sl = float(risk_zone)
        r = sw1_hi - sw1_lo
        tp1 = entry - r * 0.382
        tp2 = entry - r * 0.618
        tp3 = entry - r * 1.0
    return float(tp1), float(tp2), float(tp3), float(sl)

# =========================
# ANALYSIS PRINCIPAL
# =========================
def analyze_symbol(symbol: str):
    mode_name = MODE["current"]
    cfg = MODE_CONFIG.get(mode_name, MODE_CONFIG["normal"])

    # Timeframes
    tf_swing = "4h"
    tf_macro = "1h"
    tf_conf  = "30m"
    tf_trig  = "15m"
    tf_exec  = "5m"

    df4  = fetch_ohlcv_first_ok(symbol, tf_swing, limit=300)
    df1  = fetch_ohlcv_first_ok(symbol, tf_macro, limit=300)
    df30 = fetch_ohlcv_first_ok(symbol, tf_conf,  limit=300)
    df15 = fetch_ohlcv_first_ok(symbol, tf_trig,  limit=300)
    df5  = fetch_ohlcv_first_ok(symbol, tf_exec,  limit=300)

    if any(d.empty for d in [df4, df1, df30, df15, df5]):
        return None

    # 1) Swing 4H
    swing_dir = trend_4h(df4)
    if swing_dir == 0:
        return None  # sin swing claro

    # 2) Macro 1H obligatorio
    macro_dir = trend_tf_emas(df1, bias_hint=swing_dir)
    if macro_dir == 0 or macro_dir != swing_dir:
        return None

    side = "LONG" if macro_dir > 0 else "SHORT"

    # 3) Confirm 30m (suma score)
    conf_dir = trend_tf_emas(df30, bias_hint=macro_dir)
    score = 0.0
    trend_notes = []

    if conf_dir == macro_dir:
        score += 20
        trend_notes.append("30m alineado con 1H/4H")
    else:
        trend_notes.append("30m neutro / en contra")

    # 4) Gatillo 15m → pullback + EMA50 + RSI
    last15 = df15.iloc[-1]
    price15 = float(last15["close"])
    sw15_hi = df15["high"].tail(80).max()
    sw15_lo = df15["low"].tail(80).min()

    fib_ok, fib_txt = fib_pullback_ok(price15, sw15_lo, sw15_hi, mode_name)
    if not fib_ok:
        return None
    score += 20

    rsi_ok, rsi_txt = rsi_hook(df15, side, mode_name)
    if not rsi_ok:
        return None
    score += 15

    ema_ok, ema_txt = ema_touch(df15, side, mode_name)
    if not ema_ok:
        return None
    score += 15

    zona = premium_discount(price15, sw15_lo, sw15_hi)
    if side == "LONG" and zona == "DISCOUNT":
        score += 15
    elif side == "SHORT" and zona == "PREMIUM":
        score += 15
    elif zona == "EQ":
        # rango: revisa hacia dónde empuja
        rbias = detect_range_bias(df15)
        if (side == "LONG" and rbias > 0) or (side == "SHORT" and rbias < 0):
            score += 5
        else:
            score -= 5

    zona_valor = zona

    # 5) Exec 5m: micro confirm
    micro_ok, micro_txt = micro_confirm_5m(df5, side)
    if not micro_ok:
        return None
    score += 10

    # 6) Patrones simples en 15m
    two_top, two_bot = double_top_bottom(df15, bars=40, tol=0.004)
    patt = []
    if side == "LONG" and two_bot:
        score += 5
        patt.append("Doble Suelo 15m")
    if side == "SHORT" and two_top:
        score += 5
        patt.append("Doble Techo 15m")
    patt_txt = ", ".join(patt) if patt else "Sin patrón fuerte"

    # check score vs modo
    if score < cfg["min_score"]:
        return None

    # 7) TP/SL desde swing 1H
    entry = float(df5["close"].iloc[-1])
    tp1, tp2, tp3, sl = compute_tp_sl_from_1h(side, entry, df1, df15, df5)

    # RR mínimo razonable
    risk = abs(entry - sl)
    rr1  = abs(tp1 - entry) / max(risk, 1e-9)
    if rr1 < 1.3:  # no queremos RR basura
        return None

    ctx = {
        "zona_valor": zona_valor,
        "fib_txt": fib_txt,
        "rsi_txt": rsi_txt,
        "ema_txt": ema_txt,
        "patt_txt": patt_txt,
        "trend_txt": "; ".join(trend_notes + [micro_txt])
    }

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tf_exec": tf_exec,
        "score": score,
        "ctx": ctx
    }

# =========================
# Registro + Gmail
# =========================
def register_signal(d: dict):
    x = dict(d)
    x["ts"] = datetime.now(UTC).isoformat()
    DAILY_SIGNALS.append(x)

def send_email_with_attachments(subject: str, body: str, attachments: list):
    if not (SMTP_EMAIL and SMTP_APP_PASSWORD and SMTP_TO):
        print("⚠️ SMTP no configurado; salto envío.")
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
    day = datetime.now(UTC).strftime("%Y-%m-%d")
    send_email_with_attachments(
        f"[{PROJECT_NAME}] Señales {day}",
        f"Adjunto Excel con señales del {day} (UTC).",
        [(f"signals_{day}.xlsx",
          buf.read(),
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
    )
    DAILY_SIGNALS.clear()

# =========================
# Comandos Telegram
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MONITOR_ACTIVE
    MONITOR_ACTIVE = True
    await send_tg("✅ Bot ACTIVADO (seguimiento continuo).")

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MONITOR_ACTIVE
    MONITOR_ACTIVE = False
    await send_tg("🛑 Bot DETENIDO (no se escanean pares).")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_tg(
        f"📊 <b>ESTADO</b>\n"
        f"• Modo: <b>{MODE['current'].upper()}</b>\n"
        f"• Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"• Monitoreo: <b>{'ACTIVO' if MONITOR_ACTIVE else 'DETENIDO'}</b>"
    )

async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip().lower().split()
    if len(txt) >= 2 and txt[1] in ("soft","normal","sniper"):
        MODE["current"] = txt[1]
        await send_tg(f"⚙️ Modo cambiado a <b>{MODE['current'].upper()}</b>.")
    else:
        await send_tg("Usa: <code>/mode soft</code>, <code>/mode normal</code> o <code>/mode sniper</code>")

# =========================
# Loops principales
# =========================
async def monitor_loop():
    await startup_notice()
    while True:
        if not MONITOR_ACTIVE:
            await asyncio.sleep(3)
            continue
        print(f"[{datetime.now(UTC).strftime('%H:%M:%S')}] Escaneando {len(ACTIVE_PAIRS)} pares…")
        for sym in ACTIVE_PAIRS:
            try:
                res = analyze_symbol(sym)
                if not res:
                    continue
                side = res["side"]
                if not can_send(sym, side):
                    continue

                msg = build_signal_message(
                    res["symbol"], res["side"], res["tf_exec"],
                    res["entry"], res["sl"],
                    res["tp1"], res["tp2"], res["tp3"],
                    res["score"], res["ctx"]
                )
                await send_tg(msg)
                register_signal(res)
                mark_sent(sym, side)
            except Exception as e:
                print(f"⚠️ Error analizando {sym}: {e}")
        await asyncio.sleep(RUN_EVERY_SEC)

async def scheduler_loop():
    last_day = None
    while True:
        now = datetime.now(UTC)
        day = now.date()
        if last_day is None:
            last_day = day
        # al cambio de día (00:05 UTC) manda el Excel
        if day != last_day and now.hour == 0 and now.minute >= 5:
            try:
                await email_daily_signals_excel()
            except Exception as e:
                print("email_daily_signals_excel error:", e)
            last_day = day
        await asyncio.sleep(30)

# =========================
# FastAPI (keep-alive / TV hook)
# =========================
app = FastAPI()

@app.get("/ping")
async def ping():
    return {"ok": True, "service": PROJECT_NAME, "time": datetime.now(UTC).isoformat()}

@app.post("/tv")
async def tv_webhook(req: Request):
    if not TV_SECRET:
        return {"ok": False, "error": "TV_SECRET not set"}
    data = await req.json()
    if data.get("secret") != TV_SECRET:
        return {"ok": False, "error": "bad secret"}
    symbol = data.get("symbol")
    price  = data.get("price")
    tf     = data.get("tf") or data.get("interval")
    ts     = data.get("time") or datetime.now(UTC).isoformat()
    msg = f"📈 TV Signal\nSymbol: {symbol}\nTF: {tf}\nPrice: {price}\nTime: {ts}"
    await send_tg(msg)
    register_signal({"symbol":symbol,"side":"TV","entry":float(price or 0), "sl":0,"tp1":0,"tp2":0,"tp3":0,"tf_exec":tf,"score":0,"ctx":{},"source":"tv"})
    return {"ok": True}

def start_http():
    def _run():
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT","8080")), log_level="warning")
    th = threading.Thread(target=_run, daemon=True)
    th.start()

def start_telegram_bot():
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ Falta TELEGRAM_BOT_TOKEN, no arranco Telegram.")
        return
    app_tg = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app_tg.add_handler(CommandHandler("start",  cmd_start))
    app_tg.add_handler(CommandHandler("stop",   cmd_stop))
    app_tg.add_handler(CommandHandler("status", cmd_status))
    app_tg.add_handler(CommandHandler("mode",   cmd_mode))

    def _run():
        app_tg.run_polling()
    th = threading.Thread(target=_run, daemon=True)
    th.start()

async def main_async():
    start_http()
    start_telegram_bot()
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
