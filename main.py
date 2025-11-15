# =========================
# Bot de Señales V2 — Replit/Render 24/7
# - Multi-exchange (ccxt)
# - Multi-timeframe 4h / 1h / 15m / 5m
# - Modos: suave / normal / sniper
# - Señales por Telegram + Excel diario + comando /excel
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

# Gmail para adjuntar CSV/Excel
SMTP_EMAIL        = os.getenv("SMTP_EMAIL", "")
SMTP_APP_PASSWORD = os.getenv("SMTP_APP_PASSWORD", "")
SMTP_TO           = os.getenv("SMTP_TO", SMTP_EMAIL)

PROJECT_NAME = os.getenv("PROJECT_NAME", "BotSenalesV2")

EX_NAMES       = [s.strip() for s in os.getenv("EXCHANGES", "okx,kucoin,bybit,binance").split(",")]
PAIRS_ENV      = os.getenv("PAIRS", "")
TIMEFRAMES_ENV = os.getenv("TIMEFRAMES", "5m,15m")
MAX_PAIRS_ENV  = int(os.getenv("MAX_PAIRS", "150"))
TV_SECRET      = os.getenv("TV_SECRET", "")

# =========================
# Parámetros generales
# =========================
FIB_RET = [0.382, 0.5, 0.618, 0.786]
FIB_EXT = [0.618, 0.75, 1.0, 1.272, 1.414, 1.618, 2.0, 2.272, 2.618]
FIB_TIME_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618, 2.618]
TIME_TOL_BARS   = 2

RET_TOL   = 0.004   # 0.4% sniper
ATR_MULT  = 1.8
RUN_EVERY_SEC = 300  # escaneo cada 5 min
MIN_RR    = 1.8      # RR mínimo

# 3 modos: suave / normal / sniper
CONFIG = {
    "MODES": {
        # Macro:
        # 4h = "Swing"
        # 1h = "Macro"
        # 30m = "Confirm" (si quisieras usarlo luego)
        # Aquí usamos 4h/1h/15m/5m, pero el esquema permite 30m si se quiere extender.
        "intraday_5m":  {"MACRO":"4h", "CONFIRM":"1h", "CONFIRM2":"15m", "EXECUTION":"5m"},
        "intraday_15m": {"MACRO":"4h", "CONFIRM":"1h", "CONFIRM2":"15m", "EXECUTION":"15m"},
    },
    "ACTIVE_MODES": ["intraday_5m"],  # la de 5m es la principal, puedes añadir "intraday_15m" si quieres
    "EMA": {"fast":15, "slow":50, "long":200, "ultra_fast":12},
    "RSI": {"period":14, "hi":70, "lo":30},
    "CONSOLIDATION_BARS": 4
}

# modos de sensibilidad
# - suave: filtros más relajados -> más señales
# - normal: intermedio
# - sniper: lo más estricto
MODE = {
    "current": "normal"  # "suave", "normal", "sniper"
}

STATE = {
    "started": False,
    "last_sent": {}  # (symbol, side) -> timestamp
}

MONITOR_ACTIVE = True

# Parámetros relax / sensibilidad
RELAX = {
    # en "suave"
    "RET_TOL_SUAVE": 0.008,   # ±0.8%
    "EMA_SOFT_EPS_SUAVE": 0.002,  # 0.2%
    "RSI_LONG_SUAVE":  (30, 65),
    "RSI_SHORT_SUAVE": (35, 70),

    # en "normal"
    "RET_TOL_NORMAL": 0.006,   # ±0.6%
    "EMA_SOFT_EPS_NORMAL": 0.001,  # 0.1%
    "RSI_LONG_NORMAL":  (35, 60),
    "RSI_SHORT_NORMAL": (40, 65),

    # en "sniper" usamos RET_TOL y filtros duros
    "ALLOW_NO_FVG_NORMAL": True
}

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
    # filtro de shitcoins rarísimas
    if re.match(r"^(1000|1M|1MB|10K|B-|.*-).*?/USDT$", symbol):
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

# ===== PARES (MODO MIXTO: auto + fallback manual) =====
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

TIMEFRAMES  = [s.strip() for s in TIMEFRAMES_ENV.split(",") if s.strip()]
print(f"📦 Pares a monitorear: {len(ACTIVE_PAIRS)} (mixto={'sí' if USER_PAIRS else 'no'})")

# =========================
# Telegram helpers
# =========================
async def send_tg(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados.")
        return
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        print("❌ Telegram error:", e)

async def _startup_notice():
    modes_exec = ", ".join([CONFIG["MODES"][m]["EXECUTION"] for m in CONFIG["ACTIVE_MODES"]])
    await send_tg(
        f"✅ <b>{PROJECT_NAME}</b> iniciado\n"
        f"🧭 Modo: <b>{MODE['current'].upper()}</b>\n"
        f"🔎 Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"⏱️ Escaneo cada <b>{RUN_EVERY_SEC//60} min</b>\n"
        f"🧩 Marcos: 4h / 1h → 15m → {modes_exec}"
    )

def can_send(pair, direction):
    key = (pair, direction)
    t = STATE["last_sent"].get(key, 0)
    # antispam 30 min
    return (time.time() - t) > (30*60)

def mark_sent(pair, direction):
    STATE["last_sent"][(pair, direction)] = time.time()

def fmt_price(x):
    try:
        return f"{x:.6f}" if x < 1 else (f"{x:.4f}" if x < 100 else f"{x:.2f}")
    except:
        return str(x)

# =========================
# Formato de alerta de señal
# =========================
def build_alert(symbol, side, price, sl, tp1, tp2, zona_valor, confirmacion, mode_cfg, notes):
    if side == "LONG":
        direccion = "🟢 LONG 📈"
    else:
        direccion = "🔴 SHORT 📉"

    return (
        f"✨ <b>SEÑAL DE TRADING</b> ✨\n\n"
        f"💰 <b>ACTIVO:</b> {symbol}\n"
        f"📉 <b>TEMPORALIDAD:</b> {mode_cfg['CONFIRM']} / {mode_cfg['CONFIRM2']}\n"
        f"📍 <b>ZONA DE VALOR:</b> {zona_valor}\n"
        f"🔄 <b>CONFIRMACIÓN:</b> {confirmacion}\n"
        f"➡️ <b>DIRECCIÓN:</b> {direccion}\n"
        f"📊 <b>ENTRADA (Entry):</b> <code>{fmt_price(price)}</code>\n"
        f"🛑 <b>STOP LOSS:</b> <code>{fmt_price(sl)}</code>\n"
        f"🎯 <b>TP1:</b> <code>{fmt_price(tp1)}</code>\n"
        f"🎯 <b>TP2:</b> <code>{fmt_price(tp2)}</code>\n"
        f"🎯 <b>TP3:</b> <code>{fmt_price(tp2 + (tp2 - tp1))}</code>\n\n"
        f"📝 <b>NOTAS:</b> {', '.join(notes)}\n\n"
        f"⚠️ <b>Gestión:</b> mover SL a BE al alcanzar TP1. Opera bajo tu propio riesgo. 🍀"
    )

# =========================
# EMA / RSI / ATR nativos
# =========================
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
    ema_f = CONFIG["EMA"]["fast"]
    ema_s = CONFIG["EMA"]["slow"]
    ema_l = CONFIG["EMA"]["long"]
    ema_uf = CONFIG["EMA"]["ultra_fast"]

    df[f"EMA_{ema_f}"]  = ema(df["close"], ema_f)
    df[f"EMA_{ema_s}"]  = ema(df["close"], ema_s)
    df[f"EMA_{ema_l}"]  = ema(df["close"], ema_l)
    df[f"EMA_{ema_uf}"] = ema(df["close"], ema_uf)

    df["RSI"] = rsi(df["close"], CONFIG["RSI"]["period"])

    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()

    return df.dropna()

# =========================
# Patrones / Fibo / Bias
# =========================
def fetch_ohlcv_first_ok(symbol, timeframe, limit=500):
    for _, ex in EX_OBJS.items():
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if data and len(data) >= 50:
                df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
                # ignorar vela actual en formación
                df = df.iloc[:-1].copy()
                df["ts"] = pd.to_datetime(df["ts"], unit="ms")
                df.set_index("ts", inplace=True)
                return df
        except Exception as e:
            continue
    return pd.DataFrame()

def choch_bos_df(df, bias_hint=0):
    if len(df) < 40:
        return {"BOS_up":False,"BOS_down":False,"CHoCH_up":False,"CHoCH_down":False}
    highs = df["high"].tail(40)
    lows  = df["low"].tail(40)
    prev_hi = highs.iloc[:-1].max()
    prev_lo = lows.iloc[:-1].min()
    last = df["close"].iloc[-1]
    BOS_up = last > prev_hi
    BOS_down = last < prev_lo
    CHoCH_up = (bias_hint <= 0) and BOS_up
    CHoCH_down = (bias_hint >= 0) and BOS_down
    return {"BOS_up":BOS_up,"BOS_down":BOS_down,"CHoCH_up":CHoCH_up,"CHoCH_down":CHoCH_down}

def find_fvg_df(df, max_scan=60):
    if len(df) < 3:
        return None
    hi = df["high"].values
    lo = df["low"].values
    for i in range(len(df) - 1, max(1, len(df) - max_scan), -1):
        if i-2 < 0:
            break
        if lo[i] > hi[i-2]:
            return {"type":"bull","gap":(hi[i-2], lo[i])}
        if hi[i] < lo[i-2]:
            return {"type":"bear","gap":(hi[i], lo[i-2])}
    return None

def bullish_engulfing(df):
    if len(df) < 2:
        return False
    o1,c1,l1,h1 = df["open"].iloc[-2], df["close"].iloc[-2], df["low"].iloc[-2], df["high"].iloc[-2]
    o2,c2,l2,h2 = df["open"].iloc[-1], df["close"].iloc[-1], df["low"].iloc[-1], df["high"].iloc[-1]
    return (c2>o2) and (c1<o1) and (o2<=c1) and (c2>=o1) and (h2>=h1) and (l2<=l1)

def bearish_engulfing(df):
    if len(df) < 2:
        return False
    o1,c1,l1,h1 = df["open"].iloc[-2], df["close"].iloc[-2], df["low"].iloc[-2], df["high"].iloc[-2]
    o2,c2,l2,h2 = df["open"].iloc[-1], df["close"].iloc[-1], df["low"].iloc[-1], df["high"].iloc[-1]
    return (c2<o2) and (c1>o1) and (o2>=c1) and (c2<=o1) and (h2>=h1) and (l2<=l1)

def double_top_bottom(df, bars=40, tol=0.004):
    seg = df.tail(bars)
    hi_vals = seg["high"]
    lo_vals = seg["low"]
    max1 = hi_vals.max()
    min1 = lo_vals.min()
    last_half_hi = hi_vals.tail(bars//2).max()
    last_half_lo = lo_vals.tail(bars//2).min()
    two_top = abs(last_half_hi - max1)/max(max1,1e-9) <= tol
    two_bot = abs(last_half_lo - min1)/max(min1,1e-9) <= tol
    return two_top, two_bot, max1, min1

def flag_triangle(df, bars=30, slope_tol=0.0):
    seg = df.tail(bars).copy()
    highs = seg["high"].values
    lows  = seg["low"].values
    dec_hi = all(highs[i] <= highs[i-1] + slope_tol for i in range(1,len(highs)))
    inc_lo = all(lows[i]  >= lows[i-1]  - slope_tol for i in range(1,len(lows)))
    if dec_hi and inc_lo:
        last_close = seg["close"].iloc[-1]
        broke_up   = last_close > max(highs[:-1])
        broke_down = last_close < min(lows[:-1])
        return True, broke_up, broke_down
    return False, False, False

def fib_levels(lo, hi, is_long=True):
    base = (hi - lo) if is_long else (lo - hi)
    if base <= 0:
        return {"ret":{}, "ext":{}}
    if is_long:
        rets = {r: hi - base*r for r in FIB_RET}
        exts = {r: hi + base*r for r in FIB_EXT}
    else:
        rets = {r: lo + base*r for r in FIB_RET}
        exts = {r: lo - base*r for r in FIB_EXT}
    return {"ret":rets, "ext":exts}

def fib_time_windows(a_idx, b_idx, levels=None):
    if levels is None:
        levels = FIB_TIME_LEVELS
    dur = abs(int(b_idx) - int(a_idx))
    if dur <= 0:
        return []
    windows = [int(round(b_idx + L * dur)) for L in levels]
    return sorted(list(set(windows)))

def find_valid_block(df, is_long, bars=50):
    if len(df) < bars:
        return None
    df_scan = df.tail(bars).iloc[:-1]
    for i in range(len(df_scan)-1, 0, -1):
        c, o = df_scan["close"].iloc[i], df_scan["open"].iloc[i]
        if is_long:
            if c > o:
                ob_candle = df_scan.iloc[i-1]
                if ob_candle["close"] < ob_candle["open"]:
                    return {"type":"OB_bull","level":ob_candle["open"]}
        else:
            if c < o:
                ob_candle = df_scan.iloc[i-1]
                if ob_candle["close"] > ob_candle["open"]:
                    return {"type":"OB_bear","level":ob_candle["open"]}
    return None

def idx_near_any(now_idx, windows, tol=TIME_TOL_BARS):
    return any(abs(now_idx - w) <= tol for w in windows)

def trend_sign_from_emas(df):
    ema_f = CONFIG["EMA"]["fast"]
    ema_s = CONFIG["EMA"]["slow"]
    ema_l = CONFIG["EMA"]["long"]
    if len(df) < 60:
        return 0
    c = df["close"].iloc[-1]
    emaf = df[f"EMA_{ema_f}"].iloc[-1]
    emas = df[f"EMA_{ema_s}"].iloc[-1]
    emal = df[f"EMA_{ema_l}"].iloc[-1]
    emas_prev = df[f"EMA_{ema_s}"].iloc[-5]
    up = (c > emal) and (emaf > emas) and (df[f"EMA_{ema_s}"].iloc[-1] > emas_prev)
    dn = (c < emal) and (emaf < emas) and (df[f"EMA_{ema_s}"].iloc[-1] < emas_prev)
    return 1 if up else (-1 if dn else 0)

def premium_discount_zone(price, swing_low, swing_high):
    if swing_high <= swing_low:
        return "eq", (swing_high + swing_low)/2.0
    rango = swing_high - swing_low
    limite_discount = swing_low + rango*0.2
    limite_premium  = swing_high - rango*0.2
    if price < limite_discount:
        return "discount", (swing_high + swing_low)/2.0
    if price > limite_premium:
        return "premium", (swing_high + swing_low)/2.0
    return "eq", (swing_high + swing_low)/2.0

def sl_tp(price, is_long, sw5_lo, sw5_hi, sw15_lo, sw15_hi, atr_val, fvg=None, exts_15=None, exts_1h=None):
    base = max(1e-9, (sw15_hi - sw15_lo) if is_long else (sw15_lo - sw15_hi))
    inv_786_long  = sw15_hi - base*0.786
    inv_786_short = sw15_lo + base*0.786
    if is_long:
        sl = min(sw5_lo, inv_786_long, price - ATR_MULT*atr_val)
        cands = []
        if fvg and fvg.get("type")=="bull":
            cands.append(fvg["gap"][1])
        if exts_15:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_15:
                    cands.append(exts_15[r])
        if exts_1h:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_1h:
                    cands.append(exts_1h[r])
        cands = sorted({x for x in cands if x and x>price})
        tp1 = cands[0] if cands else price + max(atr_val*1.2, price*0.003)
        tp2 = cands[1] if len(cands)>1 else price + max(atr_val*2.0, price*0.006)
    else:
        sl = max(sw5_hi, inv_786_short, price + ATR_MULT*atr_val)
        cands = []
        if fvg and fvg.get("type")=="bear":
            cands.append(fvg["gap"][0])
        if exts_15:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_15:
                    cands.append(exts_15[r])
        if exts_1h:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_1h:
                    cands.append(exts_1h[r])
        cands = sorted({x for x in cands if x and x<price}, reverse=True)
        tp1 = cands[0] if cands else price - max(atr_val*1.2, price*0.003)
        tp2 = cands[1] if len(cands)>1 else price - max(atr_val*2.0, price*0.006)
    return float(tp1), float(tp2), float(sl)

# =========================
# Núcleo de análisis por símbolo
# =========================
def analyze_symbol(symbol, mode_cfg):
    df4  = fetch_ohlcv_first_ok(symbol, mode_cfg["MACRO"],    limit=300)
    df1  = fetch_ohlcv_first_ok(symbol, mode_cfg["CONFIRM"],  limit=300)
    df15 = fetch_ohlcv_first_ok(symbol, mode_cfg["CONFIRM2"], limit=300)
    dfE  = fetch_ohlcv_first_ok(symbol, mode_cfg["EXECUTION"],limit=300)
    if df4.empty or df1.empty or df15.empty or dfE.empty:
        return None

    df4  = compute_indicators(df4)
    df1  = compute_indicators(df1)
    df15 = compute_indicators(df15)
    dfE  = compute_indicators(dfE)

    # ===== 1) Swing 4h (EMA_100 / EMA_50) =====
    # Usamos EMA_50 como proxy de 100 si quieres, o puedes añadir explícita
    ema4_50 = ema(df4["close"], 50)
    df4["EMA_50_SWING"] = ema4_50
    price_4h = df4["close"].iloc[-1]
    swing_bias = 1 if price_4h > df4["EMA_50_SWING"].iloc[-1] else -1

    # ===== 2) Macro 1h (12/50/200) =====
    t1 = trend_sign_from_emas(df1)
    if t1 == 1:
        macro_bias = 1
    elif t1 == -1:
        macro_bias = -1
    else:
        return None

    # combinamos: si difieren demasiado, descartamos
    if swing_bias != macro_bias:
        return None

    is_long = (macro_bias == 1)
    bias_str = "LONG" if is_long else "SHORT"

    notes = [f"4h bias: {'LONG' if swing_bias==1 else 'SHORT'}",
             f"1h bias ok ({'LONG' if macro_bias==1 else 'SHORT'})",
             f"Modo: {MODE['current'].upper()}"]

    # ===== 3) Confirmación 15m (pullback + Fibo + EMA50 + RSI) =====
    last15 = df15.iloc[-1]
    c15    = float(last15["close"])
    sw15_lo = df15["low"].tail(80).min()
    sw15_hi = df15["high"].tail(80).max()
    fib15  = fib_levels(sw15_lo, sw15_hi, is_long=is_long)
    rets15 = [fib15["ret"].get(r) for r in [0.5, 0.618, 0.65, 0.75]]  # golden zone extendida

    mode = MODE["current"]
    if mode == "suave":
        tol_pull = RELAX["RET_TOL_SUAVE"]
        eps_ema  = RELAX["EMA_SOFT_EPS_SUAVE"]
        rsi_long_lo, rsi_long_hi = RELAX["RSI_LONG_SUAVE"]
        rsi_short_lo, rsi_short_hi = RELAX["RSI_SHORT_SUAVE"]
    elif mode == "normal":
        tol_pull = RELAX["RET_TOL_NORMAL"]
        eps_ema  = RELAX["EMA_SOFT_EPS_NORMAL"]
        rsi_long_lo, rsi_long_hi = RELAX["RSI_LONG_NORMAL"]
        rsi_short_lo, rsi_short_hi = RELAX["RSI_SHORT_NORMAL"]
    else:  # sniper
        tol_pull = RET_TOL
        eps_ema  = 0.0005
        rsi_long_lo, rsi_long_hi = (42, 68)
        rsi_short_lo, rsi_short_hi = (32, 58)

    def _near(vals, price, tol):
        vals = [v for v in vals if v is not None]
        return any(abs(price - v)/max(v,1e-9) <= tol for v in vals)

    near_ret15 = _near(rets15, c15, tol_pull)

    if is_long:
        cond_ema = (c15 > last15["EMA_200"]*(1 - eps_ema)) and (last15["EMA_15"] > last15["EMA_50"]*(1 - eps_ema))
        cond_rsi = (rsi_long_lo <= last15["RSI"] <= rsi_long_hi)
        if not (near_ret15 and cond_ema and cond_rsi):
            return None
    else:
        cond_ema = (c15 < last15["EMA_200"]*(1 + eps_ema)) and (last15["EMA_15"] < last15["EMA_50"]*(1 + eps_ema))
        cond_rsi = (rsi_short_lo <= last15["RSI"] <= rsi_short_hi)
        if not (near_ret15 and cond_ema and cond_rsi):
            return None

    notes.append("Pullback 15m en zona Fibo 0.5–0.75")

    # Liquidez + Order Block
    sw_prev_lo = df15["low"].iloc[:-80].min()
    sw_prev_hi = df15["high"].iloc[:-80].max()
    liquidity_taken = False
    if is_long:
        liquidity_taken = sw15_lo < sw_prev_lo * (1 - 0.001)
        if liquidity_taken:
            notes.append("Sell-side liquidez tomada")
    else:
        liquidity_taken = sw15_hi > sw_prev_hi * (1 + 0.001)
        if liquidity_taken:
            notes.append("Buy-side liquidez tomada")

    if mode == "sniper" and not liquidity_taken:
        notes.append("⚠️ Falta sweep de liquidez")
        return None

    block15 = find_valid_block(df15, is_long, bars=50)
    near_block = False
    if block15:
        near_block = _near([block15["level"]], c15, tol_pull)
        if near_block:
            notes.append(f"{block15['type']} cerca")

    if mode == "sniper" and not near_block:
        notes.append("⚠️ Falta Order Block")
        return None

    # ===== 4) Gatillo 5m/15m (hook EMA50 + RSI + patrones) =====
    def trigger_exec(df_exec, is_long, mode):
        df_exec = compute_indicators(df_exec.copy())
        if len(df_exec) < 20:
            return False, {}
        last = df_exec.iloc[-1]
        eps_local = eps_ema  # reusar

        ema_uf_val = last["EMA_12"]
        ema50_val  = last["EMA_50"]
        ema200_val = last["EMA_200"]
        rsi_val    = last["RSI"]
        close_val  = last["close"]

        # hook: precio regresando a EMA50 y girando RSI
        hook = abs(close_val - ema50_val)/max(ema50_val,1e-9) <= tol_pull

        # estructura
        struct = choch_bos_df(df_exec, bias_hint=(1 if is_long else -1))
        fvg    = find_fvg_df(df_exec, max_scan=50)

        if is_long:
            cond = (
                close_val > ema200_val*(1 - eps_local) and
                ema_uf_val > ema50_val*(1 - eps_local) and
                rsi_val > 50 and
                (struct["BOS_up"] or struct["CHoCH_up"]) and
                hook
            )
            if mode == "sniper":
                cond = cond and (not fvg or fvg["type"]=="bull")
        else:
            cond = (
                close_val < ema200_val*(1 + eps_local) and
                ema_uf_val < ema50_val*(1 + eps_local) and
                rsi_val < 50 and
                (struct["BOS_down"] or struct["CHoCH_down"]) and
                hook
            )
            if mode == "sniper":
                cond = cond and (not fvg or fvg["type"]=="bear")

        return cond, {"struct":struct, "fvg":fvg, "price":float(close_val), "atr":float(last["ATR"])}

    if mode_cfg["EXECUTION"] == "5m":
        ok_exec, extra = trigger_exec(dfE, is_long, mode)
    else:
        ok_exec, extra = trigger_exec(df15, is_long, mode)

    if not ok_exec:
        return None

    # patrones extra en exec
    if mode_cfg["EXECUTION"] == "5m":
        df_pat = dfE
    else:
        df_pat = df15

    bull_eng = bullish_engulfing(df_pat)
    bear_eng = bearish_engulfing(df_pat)
    two_top, two_bot, _, _ = double_top_bottom(df_pat, bars=40, tol=0.004)
    tri_ok, broke_up, broke_dn = flag_triangle(df_pat, bars=30)

    if is_long:
        patt_ok = bull_eng or two_bot or (tri_ok and broke_up)
    else:
        patt_ok = bear_eng or two_top or (tri_ok and broke_dn)

    if mode == "sniper" and not patt_ok:
        notes.append("⚠️ Falta patrón fuerte en gatillo")
        return None
    elif patt_ok:
        notes.append("Patrón gatillo OK")

    # ===== premium / discount en 15m =====
    zone15, eq15 = premium_discount_zone(c15, sw15_lo, sw15_hi)
    if is_long:
        if zone15 == "premium":
            notes.append("⚠️ Long en zona premium (descartado)")
            return None
        if zone15 == "eq":
            notes.append("Zona Eq (cuidado)")
    else:
        if zone15 == "discount":
            notes.append("⚠️ Short en zona discount (descartado)")
            return None
        if zone15 == "eq":
            notes.append("Zona Eq (cuidado)")

    zona_valor_reporte = zone15.upper()

    # ===== SL / TPs =====
    if mode_cfg["EXECUTION"] == "5m":
        sw5_lo = dfE["low"].tail(60).min()
        sw5_hi = dfE["high"].tail(60).max()
    else:
        sw5_lo = df15["low"].tail(60).min()
        sw5_hi = df15["high"].tail(60).max()

    price_exec = extra.get("price", float(dfE["close"].iloc[-1]))
    atr_exec   = extra.get("atr", float(dfE["ATR"].iloc[-1]))

    sw1_lo = df1["low"].tail(120).min()
    sw1_hi = df1["high"].tail(120).max()
    fib_ext_15 = fib_levels(sw15_lo, sw15_hi, is_long=is_long)["ext"]
    fib_ext_1h = fib_levels(sw1_lo,  sw1_hi,  is_long=is_long)["ext"]

    tp1, tp2, sl = sl_tp(
        price_exec, is_long,
        sw5_lo, sw5_hi,
        sw15_lo, sw15_hi,
        atr_exec,
        fvg=extra.get("fvg"),
        exts_15=fib_ext_15,
        exts_1h=fib_ext_1h
    )

    # ===== RR =====
    risk = abs(price_exec - sl)
    reward1 = abs(tp1 - price_exec)
    if risk <= 1e-9:
        return None
    rr1 = reward1 / risk

    rr_min = MIN_RR
    if mode == "suave":
        rr_min = 1.2
    elif mode == "normal":
        rr_min = 1.6
    else:
        rr_min = MIN_RR

    if rr1 < rr_min:
        notes.append(f"⚠️ RR {rr1:.2f} (baja)")
        if mode == "sniper":
            return None
    else:
        notes.append(f"RR {rr1:.2f} (ok)")

    # FVG y estructura a notas
    st = extra.get("struct", {})
    fvg = extra.get("fvg")
    if fvg:
        notes.append(f"FVG {fvg['type']}")
    if st.get("BOS_up"):
        notes.append("BOS↑")
    if st.get("BOS_down"):
        notes.append("BOS↓")
    if st.get("CHoCH_up"):
        notes.append("CHoCH↑")
    if st.get("CHoCH_down"):
        notes.append("CHoCH↓")

    # doble techo/suelo en 15m para confirmación
    two_top_15, two_bot_15, _, _ = double_top_bottom(df15, bars=40, tol=0.004)
    patrones_detectados = []
    if is_long:
        if two_bot_15:
            patrones_detectados.append("15m")
        confirmacion_msg = f"Doble Suelo en {', '.join(patrones_detectados)}." if patrones_detectados else "Sin patrón fuerte visible."
    else:
        if two_top_15:
            patrones_detectados.append("15m")
        confirmacion_msg = f"Doble Techo en {', '.join(patrones_detectados)}." if patrones_detectados else "Sin patrón fuerte visible."

    side = "LONG" if is_long else "SHORT"

    return {
        "side": side,
        "price": price_exec,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "notes": notes,
        "zona_valor": zona_valor_reporte,
        "confirmacion": confirmacion_msg
    }

# =========================
# Registro de señales + Gmail adjuntos
# =========================
DAILY_SIGNALS = []

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
        print("ℹ️ No hay señales para el Excel diario.")
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
        [(f"signals_{day}.xlsx", buf.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
    )
    print("📧 Excel diario enviado.")
    DAILY_SIGNALS.clear()

async def collect_candles_and_email():
    rows = []
    pairs = ACTIVE_PAIRS
    tfs   = TIMEFRAMES
    max_pairs = min(len(pairs), 20)
    for sym in pairs[:max_pairs]:
        for tf in tfs[:2]:
            df = fetch_ohlcv_first_ok(sym, tf, limit=120)
            if df.empty:
                continue
            last = df.tail(1).reset_index()
            last["symbol"] = sym
            last["tf"] = tf
            rows.append(last)
    if not rows:
        return
    out = pd.concat(rows, ignore_index=True).rename(columns={"ts":"time"})
    csv_bytes = out.to_csv(index=False).encode("utf-8")
    stamp = datetime.now(UTC).strftime("%Y-%m-%d_%H%M")
    send_email_with_attachments(
        f"[{PROJECT_NAME}] Velas {stamp}",
        f"Adjunto CSV de última vela por símbolo/TF ({stamp} UTC).",
        [(f"candles_{stamp}.csv", csv_bytes, "text/csv")]
    )
    print("📧 CSV de velas enviado.")

# =========================
# Comandos Telegram
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MONITOR_ACTIVE
    MONITOR_ACTIVE = True
    modes_exec = ", ".join([CONFIG["MODES"][m]["EXECUTION"] for m in CONFIG["ACTIVE_MODES"]])
    await send_tg(
        f"✅ Bot ACTIVADO\n"
        f"🧭 Modo: <b>{MODE['current'].upper()}</b>\n"
        f"🔎 Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"⏱️ {RUN_EVERY_SEC//60} min\n"
        f"Ejecuciones: {modes_exec}"
    )

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MONITOR_ACTIVE
    MONITOR_ACTIVE = False
    await send_tg("🛑 Bot DETENIDO.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    modes_exec = ", ".join([CONFIG["MODES"][m]["EXECUTION"] for m in CONFIG["ACTIVE_MODES"]])
    await send_tg(
        f"📊 <b>ESTADO</b>\n"
        f"• Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"• Modos exec: <b>{modes_exec}</b>\n"
        f"• Modo actual: <b>{MODE['current'].upper()}</b>\n"
        f"• Monitoreo: <b>{'ACTIVO' if MONITOR_ACTIVE else 'DETENIDO'}</b>"
    )

async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip().lower().split()
    if len(txt) >= 2 and txt[1] in ("suave","normal","sniper"):
        MODE["current"] = txt[1]
        await send_tg(f"⚙️ Modo cambiado a <b>{MODE['current'].upper()}</b>.")
    else:
        await send_tg("Usa: <code>/mode suave</code>, <code>/mode normal</code> o <code>/mode sniper</code>")

async def cmd_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_tg("⏳ Generando y enviando Excel de señales del día…")
    try:
        await email_daily_signals_excel()
        await send_tg("✅ Excel enviado (si había señales). Revisa tu correo.")
    except Exception as e:
        await send_tg(f"❌ Error enviando Excel: {e}")

# =========================
# Bucles principales
# =========================
async def monitor_loop():
    await _startup_notice()
    while True:
        if not MONITOR_ACTIVE:
            await asyncio.sleep(3)
            continue
        for mk in CONFIG["ACTIVE_MODES"]:
            mode_cfg = CONFIG["MODES"][mk]
            print(f"[{datetime.now(UTC).strftime('%H:%M:%S')}] Escaneando {mk} (EXEC {mode_cfg['EXECUTION']})…")
            for sym in ACTIVE_PAIRS:
                try:
                    res = analyze_symbol(sym, mode_cfg)
                    if not res:
                        continue

                    side = res["side"]
                    price, sl, tp1, tp2 = res["price"], res["sl"], res["tp1"], res["tp2"]
                    zona_valor, confirmacion = res["zona_valor"], res["confirmacion"]
                    notes = res["notes"]

                    if can_send(sym, side):
                        msg = build_alert(sym, side, price, sl, tp1, tp2, zona_valor, confirmacion, mode_cfg, notes)
                        await send_tg(msg)

                        full_notes = f"{zona_valor}|{confirmacion}|" + "|".join(notes)
                        register_signal({
                            "symbol": sym,
                            "side": side,
                            "price": price,
                            "sl": sl,
                            "tp1": tp1,
                            "tp2": tp2,
                            "notes": full_notes,
                            "mode": mk
                        })
                        mark_sent(sym, side)
                except Exception as e:
                    print(f"⚠️ Error analizando {sym}: {e}")
        await asyncio.sleep(RUN_EVERY_SEC)

async def scheduler_loop():
    last_csv_min = -1
    while True:
        now = datetime.now(UTC)
        # cada 30 min manda CSV de velas
        if now.minute in (0,30) and last_csv_min != now.minute:
            last_csv_min = now.minute
            try:
                await collect_candles_and_email()
            except Exception as e:
                print("collect_candles_and_email error:", e)

        # Excel diario 23:59 UTC
        if now.hour == 23 and now.minute == 59 and now.second < 5:
            try:
                await email_daily_signals_excel()
            except Exception as e:
                print("email_daily_signals_excel error:", e)
            await asyncio.sleep(5)
        await asyncio.sleep(1)

# =========================
# FastAPI keep-alive (/ping, /tv)
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
    register_signal({"source":"tv","symbol":symbol,"price":price,"tf":tf,"time":ts})
    return {"ok": True}

def start_http():
    def _run():
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT","8080")), log_level="warning")
    th = threading.Thread(target=_run, daemon=True)
    th.start()

# =========================
# Telegram (run_polling en thread)
# =========================
def start_telegram_bot():
    app_tg = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app_tg.add_handler(CommandHandler("start",  cmd_start))
    app_tg.add_handler(CommandHandler("stop",   cmd_stop))
    app_tg.add_handler(CommandHandler("status", cmd_status))
    app_tg.add_handler(CommandHandler("mode",   cmd_mode))
    app_tg.add_handler(CommandHandler("excel",  cmd_excel))

    def _run():
        app_tg.run_polling()
    th = threading.Thread(target=_run, daemon=True)
    th.start()

# =========================
# Entrypoint
# =========================
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
