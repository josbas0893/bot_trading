# =========================
# Bot de Señales V2 — Replit / Render 24/7
# Swing 4h + Macro 1h + Pullback 15m + Gatillo 5m
# Con “entrenamiento” inicial y 6 mensajes de estado
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

# Gmail solo para adjuntar CSV/Excel
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
# Parámetros de estrategia
# =========================
FIB_RET = [0.382, 0.5, 0.618, 0.786]
FIB_EXT = [0.618, 0.75, 1.0, 1.272, 1.414, 1.618, 2.0, 2.272, 2.618]
FIB_TIME_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618, 2.618]
TIME_TOL_BARS   = 2

RET_TOL   = 0.004   # 0.4%
ATR_MULT  = 1.8
RUN_EVERY_SEC = 300 # escaneo cada 5 min
MIN_RR    = 1.8     # Ratio mínimo

CONFIG = {
    "MODES": {
        # Swing 4h, Macro 1h, Pullback 15m, Ejecución 5m
        "intraday_5m":  {"MACRO":"4h", "CONFIRM":"1h", "CONFIRM2":"15m", "EXECUTION":"5m"},
    },
    "ACTIVE_MODES": ["intraday_5m"],
    "EMA": {"fast":15, "slow":50, "long":200, "ultra_fast":12}, 
    "RSI": {"period":14, "hi":70, "lo":30},
    "CONSOLIDATION_BARS": 4
}

STATE = {"started": False, "last_sent": {}}
MODE  = {"current": "normal"}     # "normal" o "sniper"
MONITOR_ACTIVE = True

RELAX = {
    "RET_TOL_NORMAL": 0.006,
    "RSI_LONG":  (35, 60),
    "RSI_SHORT": (40, 65),
    "EMA_SOFT_EPS": 0.0005,
    "ALLOW_NO_FVG_NORMAL": True
}

# para el "entrenamiento" inicial
TRAINING_SAMPLE = 50
TRAIN_APPROVED = []
TRAIN_REJECTED = []

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
    if not symbol.endswith("/USDT"): return False
    if re.match(r"^(1000|1M|1MB|10K|B-|.*-).*?/USDT$", symbol): return False
    if "PERP" in symbol.upper(): return False
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
        if not ex: continue
        syms = _collect_from(ex)
        for s in syms:
            if s not in seen:
                agg.append(s); seen.add(s)
        if len(agg) >= limit: break
    return sorted(agg)[:limit]

init_exchanges()

# ===== PARES (MODO MIXTO: auto + fallback manual) =====
USER_PAIRS = []
if PAIRS_ENV and PAIRS_ENV.strip().lower() != "auto":
    USER_PAIRS = [s.strip() for s in PAIRS_ENV.split(",") if s.strip()]

DYN = build_pairs_dynamic(limit=MAX_PAIRS_ENV)
if USER_PAIRS:
    seen = set(); ACTIVE_PAIRS = []
    for s in USER_PAIRS + DYN:
        if s not in seen:
            ACTIVE_PAIRS.append(s); seen.add(s)
    ACTIVE_PAIRS = ACTIVE_PAIRS[:MAX_PAIRS_ENV]
else:
    ACTIVE_PAIRS = DYN

TIMEFRAMES  = [s.strip() for s in TIMEFRAMES_ENV.split(",") if s.strip()]
print(f"📦 Pares a monitorear: {len(ACTIVE_PAIRS)} (mixto={'sí' if USER_PAIRS else 'no'})")

# =========================
# Telegram helpers
# =========================
async def send_tg(text):
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print("⚠️ TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados")
            return
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        print("❌ Telegram error:", e)

async def _startup_notice():
    modes = ", ".join([CONFIG["MODES"][m]["EXECUTION"] for m in CONFIG["ACTIVE_MODES"]])
    await send_tg(
        f"✅ {PROJECT_NAME} iniciado\n"
        f"🧭 Modo: <b>{MODE['current'].upper()}</b>\n"
        f"🔎 Analizando: <b>{len(ACTIVE_PAIRS)}</b> pares\n"
        f"⏱️ Escaneo cada <b>{RUN_EVERY_SEC//60} min</b>\n"
        f"🧩 Marcos: 4h / 1h / 15m → 5m"
    )

def can_send(pair, direction):
    key = (pair, direction)
    t = STATE["last_sent"].get(key, 0)
    # Antispam de 30 minutos
    return (time.time() - t) > (30*60)

def mark_sent(pair, direction):
    STATE["last_sent"][(pair, direction)] = time.time()

def fmt_price(x):
    try:
        return f"{x:.6f}" if x<1 else (f"{x:.4f}" if x<100 else f"{x:.2f}")
    except:
        return str(x)

def build_alert_v21(symbol, side, price, sl, tp1, tp2, zona_valor, confirmacion, mode_cfg, notes):
    if side == "LONG":
        direccion = "🟢 LONG 📈"
    else:
        direccion = "🔴 SHORT 📉"

    return (
        f"🤖 SEÑAL DEL BOT MONSTRUO ⚡️\n"
        f"✨ ALERTA DE TRADING ⚡️ ✨\n\n"
        f"💰 ACTIVO: {symbol}\n"
        f"📉 TEMPORALIDAD: {mode_cfg['MACRO']} / {mode_cfg['CONFIRM2']} / {mode_cfg['EXECUTION']}\n"
        f"🎯 ZONA DE VALOR: {zona_valor}\n"
        f"🔄 CONFIRMACIÓN: {confirmacion}\n"
        f"➡️ DIRECCIÓN: {direccion}\n"
        f"📊 ENTRADA (Entry): <code>{fmt_price(price)}</code>\n"
        f"🛑 STOP LOSS (SL): <code>{fmt_price(sl)}</code>\n"
        f"✅ TAKE PROFIT 1 (TP1): <code>{fmt_price(tp1)}</code>\n"
        f"✅ TAKE PROFIT 2 (TP2): <code>{fmt_price(tp2)}</code>\n"
        f"✅ TAKE PROFIT 3 (TP3): <code>{fmt_price(tp2+(tp2-tp1))}</code>\n"
        f"📝 NOTAS: {', '.join(notes)}\n\n"
        f"⚠️ Gestión: mover SL a BE en TP1. No es consejo financiero; usa tu propia gestión. 🍀"
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
    if df.empty: return df
    ema_f, ema_s, ema_l, ema_uf = CONFIG["EMA"]["fast"], CONFIG["EMA"]["slow"], CONFIG["EMA"]["long"], CONFIG["EMA"]["ultra_fast"]

    df[f"EMA_{ema_f}"] = ema(df["close"], ema_f)
    df[f"EMA_{ema_s}"] = ema(df["close"], ema_s)
    df[f"EMA_{ema_l}"] = ema(df["close"], ema_l)
    df[f"EMA_{ema_uf}"] = ema(df["close"], ema_uf)  # EMA12

    df["RSI"] = rsi(df["close"], CONFIG["RSI"]["period"])
    tr = pd.concat([
        (df["high"]-df["low"]),
        (df["high"]-df["close"].shift(1)).abs(),
        (df["low"]-df["close"].shift(1)).abs()
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
            if data and len(data) >= 20:
                df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
                df = df.iloc[:-1].copy()
                df["ts"] = pd.to_datetime(df["ts"], unit="ms")
                df.set_index("ts", inplace=True)
                return df
        except:
            continue
    return pd.DataFrame()

def choch_bos_df(df, bias_hint=0):
    if len(df) < 40:
        return {"BOS_up":False,"BOS_down":False,"CHoCH_up":False,"CHoCH_down":False}
    highs = df["high"].tail(40); lows = df["low"].tail(40)
    prev_hi = highs.iloc[:-1].max(); prev_lo = lows.iloc[:-1].min()
    last = df["close"].iloc[-1]
    BOS_up = last > prev_hi; BOS_down = last < prev_lo
    CHoCH_up = (bias_hint <= 0) and BOS_up
    CHoCH_down = (bias_hint >= 0) and BOS_down
    return {"BOS_up":BOS_up,"BOS_down":BOS_down,"CHoCH_up":CHoCH_up,"CHoCH_down":CHoCH_down}

def find_fvg_df(df, max_scan=60):
    if len(df) < 3: return None
    hi = df["high"].values; lo = df["low"].values
    for i in range(len(df)-1, max(1, len(df)-max_scan), -1):
        if i-2 < 0: break
        if lo[i] > hi[i-2]:
            return {"type":"bull","gap":(hi[i-2], lo[i])}
        if hi[i] < lo[i-2]:
            return {"type":"bear","gap":(hi[i], lo[i-2])}
    return None

def bullish_engulfing(df):
    if len(df) < 2: return False
    o1,c1,l1,h1 = df["open"].iloc[-2], df["close"].iloc[-2], df["low"].iloc[-2], df["high"].iloc[-2]
    o2,c2,l2,h2 = df["open"].iloc[-1], df["close"].iloc[-1], df["low"].iloc[-1], df["high"].iloc[-1]
    return (c2>o2) and (c1<o1) and (o2<=c1) and (c2>=o1) and (h2>=h1) and (l2<=l1)

def bearish_engulfing(df):
    if len(df) < 2: return False
    o1,c1,l1,h1 = df["open"].iloc[-2], df["close"].iloc[-2], df["low"].iloc[-2], df["high"].iloc[-2]
    o2,c2,l2,h2 = df["open"].iloc[-1], df["close"].iloc[-1], df["low"].iloc[-1], df["high"].iloc[-1]
    return (c2<o2) and (c1>o1) and (o2>=c1) and (c2<=o1) and (h2>=h1) and (l2<=l1)

def double_top_bottom(df, bars=40, tol=0.004):
    seg = df.tail(bars)
    hi_vals = seg["high"]; lo_vals = seg["low"]
    max1 = hi_vals.max(); min1 = lo_vals.min()
    last_half_hi = hi_vals.tail(bars//2).max()
    last_half_lo = lo_vals.tail(bars//2).min()

    two_top  = abs(last_half_hi-max1)/max(max1,1e-9) <= tol
    two_bot  = abs(last_half_lo-min1)/max(min1,1e-9) <= tol
    return two_top, two_bot, max1, min1

def flag_triangle(df, bars=30, slope_tol=0.0):
    seg = df.tail(bars).copy()
    highs = seg["high"].values; lows = seg["low"].values
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
    if levels is None: levels = FIB_TIME_LEVELS
    dur = abs(int(b_idx) - int(a_idx))
    if dur <= 0: return []
    windows = [int(round(b_idx + L * dur)) for L in levels]
    return sorted(list(set(windows)))

def find_valid_block(df, is_long, bars=50):
    if len(df) < bars: return None
    df_scan = df.tail(bars).iloc[:-1]
    for i in range(len(df_scan) - 1, 0, -1):
        c, o = df_scan["close"].iloc[i], df_scan["open"].iloc[i]
        if is_long:
            if c > o:
                ob_candle = df_scan.iloc[i-1]
                if ob_candle["close"] < ob_candle["open"]:
                    return {"type":"OB_bull", "level": ob_candle["open"]} 
        else:
            if c < o:
                ob_candle = df_scan.iloc[i-1]
                if ob_candle["close"] > ob_candle["open"]:
                    return {"type":"OB_bear", "level": ob_candle["open"]}
    return None

def idx_near_any(now_idx, windows, tol=TIME_TOL_BARS):
    return any(abs(now_idx - w) <= tol for w in windows)

def trend_sign_from_emas(df):
    ema_f, ema_s, ema_l = CONFIG["EMA"]["fast"], CONFIG["EMA"]["slow"], CONFIG["EMA"]["long"]
    if len(df) < 60: return 0
    c = df["close"].iloc[-1]
    emaf = df[f"EMA_{ema_f}"].iloc[-1]; emas = df[f"EMA_{ema_s}"].iloc[-1]; emal = df[f"EMA_{ema_l}"].iloc[-1]
    emas_prev = df[f"EMA_{ema_s}"].iloc[-5]
    up = (c > emal) and (emaf > emas) and (df[f"EMA_{ema_s}"].iloc[-1] > emas_prev)
    dn = (c < emal) and (emaf < emas) and (df[f"EMA_{ema_s}"].iloc[-1] < emas_prev)
    return 1 if up else (-1 if dn else 0)

def premium_discount_zone(price, swing_low, swing_high, tol=0.0):
    if swing_high <= swing_low:
        return "eq", (swing_high + swing_low)/2.0
    rango = swing_high - swing_low
    limite_discount = swing_low + rango * 0.2
    limite_premium  = swing_high - rango * 0.2 
    if price < limite_discount:  return "discount", (swing_high + swing_low)/2.0
    if price > limite_premium:   return "premium", (swing_high + swing_low)/2.0
    return "eq", (swing_high + swing_low)/2.0

# =========================
# Pullback simple en 15m
# =========================
def simple_pullback_ok(df, is_long: bool, lookback: int = 80,
                       min_pos: float = 0.25, max_pos: float = 0.75,
                       ema_len: int = 50, atr_mult_below: float = 1.5, atr_mult_above: float = 1.0):
    """
    Revisa:
    - Que el precio esté en la zona media del último swing (25%–75%).
    - Que esté cerca de la EMA (50) dentro de un rango de ATR.
    """
    if len(df) < lookback + 5:
        return False

    seg = df.tail(lookback).copy()
    last_close = float(seg["close"].iloc[-1])

    swing_low  = float(seg["low"].min())
    swing_high = float(seg["high"].max())
    if swing_high <= swing_low:
        return False

    if is_long:
        pos = (last_close - swing_low) / (swing_high - swing_low)
    else:
        pos = (swing_high - last_close) / (swing_high - swing_low)

    if not (min_pos <= pos <= max_pos):
        return False

    ema_col = f"EMA_{ema_len}"
    if ema_col not in seg.columns:
        seg[ema_col] = ema(seg["close"], ema_len)

    ema_val = float(seg[ema_col].iloc[-1])
    atr_val = float(seg["ATR"].iloc[-1]) if "ATR" in seg.columns else 0.0
    if atr_val <= 0:
        return False

    if is_long:
        lower = ema_val - atr_mult_below * atr_val
        upper = ema_val + atr_mult_above * atr_val
        return lower <= last_close <= upper
    else:
        upper = ema_val + atr_mult_below * atr_val
        lower = ema_val - atr_mult_above * atr_val
        return lower <= last_close <= upper

# =========================
# SL / TP
# =========================
def sl_tp(price, is_long, sw5_lo, sw5_hi, sw15_lo, sw15_hi, atr_val, fvg=None, exts_15=None, exts_1h=None):
    base = max(1e-9, (sw15_hi - sw15_lo) if is_long else (sw15_lo - sw15_hi))
    inv_786_long  = sw15_hi - base*0.786
    inv_786_short = sw15_lo + base*0.786
    if is_long:
        sl  = min(sw5_lo, inv_786_long, price - ATR_MULT*atr_val)
        cands = []
        if fvg and fvg.get("type")=="bull": cands.append(fvg["gap"][1])
        if exts_15:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_15: cands.append(exts_15[r])
        if exts_1h:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_1h: cands.append(exts_1h[r])
        cands = sorted({x for x in cands if x and x>price})
        tp1 = cands[0] if cands else price + max(atr_val*1.2, price*0.003)
        tp2 = cands[1] if len(cands)>1 else price + max(atr_val*2.0, price*0.006)
    else:
        sl  = max(sw5_hi, inv_786_short, price + ATR_MULT*atr_val)
        cands = []
        if fvg and fvg.get("type")=="bear": cands.append(fvg["gap"][0])
        if exts_15:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_15: cands.append(exts_15[r])
        if exts_1h:
            for r in [0.618,0.75,1.0,1.272,1.414,1.618]:
                if r in exts_1h: cands.append(exts_1h[r])
        cands = sorted({x for x in cands if x and x<price}, reverse=True)
        tp1 = cands[0] if cands else price - max(atr_val*1.2, price*0.003)
        tp2 = cands[1] if len(cands)>1 else price - max(atr_val*2.0, price*0.006)
    return float(tp1), float(tp2), float(sl)

# =========================
# Análisis principal de un símbolo
# =========================
def analyze_symbol(symbol, mode_cfg, hard_sniper=True):
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

    t4 = trend_sign_from_emas(df4)
    t1 = trend_sign_from_emas(df1)
    if t4==1 and t1>=0:
        bias = "LONG"; is_long = True
    elif t4==-1 and t1<=0:
        bias = "SHORT"; is_long = False
    else:
        return None

    notes = [f"4h manda; 1h no contradice", f"Modo: {MODE.get('current','normal').upper()}"]

    last15 = df15.iloc[-1]
    c15    = float(last15["close"])
    sw15_lo, sw15_hi = df15["low"].tail(80).min(), df15["high"].tail(80).max()
    fib15  = fib_levels(sw15_lo, sw15_hi, is_long=is_long)

    is_normal = (MODE.get("current","normal") == "normal")

    # Pullback 15m usando simple_pullback_ok
    if MODE["current"] == "sniper":
        pull_ok = simple_pullback_ok(df15, is_long,
                                     lookback=80,
                                     min_pos=0.35, max_pos=0.65,
                                     ema_len=50,
                                     atr_mult_below=1.2, atr_mult_above=0.8)
    else:
        pull_ok = simple_pullback_ok(df15, is_long,
                                     lookback=80,
                                     min_pos=0.25, max_pos=0.75,
                                     ema_len=50,
                                     atr_mult_below=1.5, atr_mult_above=1.0)
    if not pull_ok:
        notes.append("⚠️ Pullback 15m NO válido")
        return None
    else:
        notes.append("Pullback 15m OK (EMA50 + ATR)")

    # Filtro EMA/RSI sobre el pullback 15m
    if is_long:
        rsi_lo, rsi_hi = (RELAX["RSI_LONG"] if is_normal else (42,68))
        cond_ema = (c15 > last15["EMA_200"]*(1 - (RELAX["EMA_SOFT_EPS"] if is_normal else 0.0))) and \
                   (last15["EMA_15"] > last15["EMA_50"]*(1 - (RELAX["EMA_SOFT_EPS"] if is_normal else 0.0)))
        cond_rsi = (rsi_lo <= last15["RSI"] <= rsi_hi)
        if not (cond_ema and cond_rsi):
            notes.append("⚠️ EMA/RSI 15m no cumple")
            return None
    else:
        rsi_lo, rsi_hi = (RELAX["RSI_SHORT"] if is_normal else (32,58))
        cond_ema = (c15 < last15["EMA_200"]*(1 + (RELAX["EMA_SOFT_EPS"] if is_normal else 0.0))) and \
                   (last15["EMA_15"] < last15["EMA_50"]*(1 + (RELAX["EMA_SOFT_EPS"] if is_normal else 0.0)))
        cond_rsi = (rsi_lo <= last15["RSI"] <= rsi_hi)
        if not (cond_ema and cond_rsi):
            notes.append("⚠️ EMA/RSI 15m no cumple")
            return None

    # --- FILTROS SMC (liquidez + OB) ---
    sw_prev_lo, sw_prev_hi = df15["low"].iloc[:-80].min(), df15["high"].iloc[:-80].max() 
    liquidity_taken = False

    if is_long:
        liquidity_taken = sw15_lo < sw_prev_lo * (1 - 0.001) 
        if liquidity_taken: notes.append("Sell-Side Liquidez Tomada")
    else:
        liquidity_taken = sw15_hi > sw_prev_hi * (1 + 0.001)
        if liquidity_taken: notes.append("Buy-Side Liquidez Tomada")

    if hard_sniper and not liquidity_taken:
        notes.append("⚠️ Faltó Liquidez Sweep")
        return None 

    block15 = find_valid_block(df15, is_long, bars=50)
    near_block = False
    if block15:
        last_c = float(df15["close"].iloc[-1])
        level  = block15["level"]
        if abs(last_c - level)/max(level,1e-9) <= (RELAX["RET_TOL_NORMAL"] if is_normal else RET_TOL):
            near_block = True
            notes.append(f"{block15['type']} Near")

    if hard_sniper and not near_block:
        notes.append("⚠️ Faltó Order Block")
        return None 

    # --- Fibo de tiempo para 1h → 15m y 15m → 5m ---
    def last_swing_idx(df):
        seg = df.tail(120)
        lo_idx = seg["low"].idxmin()
        hi_idx = seg["high"].idxmax()
        seg_pos = df.index.get_indexer_for(seg.index)
        rel_lo = seg_pos[seg.index.get_loc(lo_idx)]
        rel_hi = seg_pos[seg.index.get_loc(hi_idx)]
        return rel_lo, rel_hi

    try:
        lo1, hi1 = last_swing_idx(df1)
        lo15, hi15 = last_swing_idx(df15)
    except:
        lo1, hi1, lo15, hi15 = 0,1,0,1

    if is_long:
        A1,B1   = lo1, hi1
        A15,B15 = lo15, hi15
    else:
        A1,B1   = hi1, lo1
        A15,B15 = hi15, lo15

    windows_1h_on_15m = []
    if B1 > A1:
        windows_1h_on_15m = fib_time_windows(A1*4, B1*4, levels=FIB_TIME_LEVELS)

    mult = 3 if mode_cfg["EXECUTION"]=="5m" else 1
    windows_15_on_exec = []
    if B15 > A15:
        windows_15_on_exec = fib_time_windows(A15*mult, B15*mult, levels=FIB_TIME_LEVELS)

    now_15, now_exec = len(df15)-1, len(dfE)-1
    time_ok_15   = idx_near_any(now_15,  windows_1h_on_15m, tol=TIME_TOL_BARS)
    time_ok_exec = idx_near_any(now_exec, windows_15_on_exec, tol=TIME_TOL_BARS)

    if hard_sniper and not (time_ok_15 or time_ok_exec):
        notes.append("⚠️ Faltó Time Fibo")
        return None 
    if time_ok_15 or time_ok_exec:
        notes.append("Time Fibo near") 

    # --- PREMIUM / DISCOUNT ---
    zone15, eq15 = premium_discount_zone(c15, sw15_lo, sw15_hi, tol=(0.0 if hard_sniper else 0.01))

    if is_long:
        if zone15=="premium" or (zone15=="eq" and abs(c15 - eq15)/c15 > 0.005): 
            notes.append(f"⚠️ Zona {zone15.upper()}")
            return None
        elif zone15=="eq": 
            notes.append("Zona Eq (Permisiva)")
    else:
        if zone15=="discount" or (zone15=="eq" and abs(c15 - eq15)/c15 > 0.005):
            notes.append(f"⚠️ Zona {zone15.upper()}")
            return None
        elif zone15=="eq": 
            notes.append("Zona Eq (Permisiva)")

    zona_valor_reporte = zone15.upper()

    extra = {}
    sw5_lo, sw5_hi = dfE["low"].tail(60).min(), dfE["high"].tail(60).max()

    # ========= GATILLO en 5m =========
    dfE = compute_indicators(dfE)

    bull_eng = bullish_engulfing(dfE)
    bear_eng = bearish_engulfing(dfE)
    two_top, two_bot, _, _ = double_top_bottom(dfE, bars=40, tol=0.004)
    tri_ok, broke_up, broke_dn = flag_triangle(dfE, bars=30)

    def trigger_5m_relaxed(df5, bias, is_normal=True):
        df5 = compute_indicators(df5.copy())
        if len(df5) < 10: 
            return False, {}
        last, prev = df5.iloc[-1], df5.iloc[-2]
        eps = RELAX["EMA_SOFT_EPS"] if is_normal else 0.0

        emauf_val = last["EMA_12"] 

        cross_up = emauf_val > last["EMA_50"] * (1 - eps)
        cross_dn = emauf_val < last["EMA_50"] * (1 + eps)
        cond_rsi_impulse_up = last["RSI"] > 50
        cond_rsi_impulse_dn = last["RSI"] < 50

        side_up  = last["close"] > last["EMA_200"]*(1 - eps)
        side_dn  = last["close"] < last["EMA_200"]*(1 + eps)

        struct = choch_bos_df(df5, bias_hint=(1 if bias=="LONG" else -1))
        fvg = find_fvg_df(df5, max_scan=50)

        if bias=="LONG":
            ok = cross_up and cond_rsi_impulse_up and side_up and (struct["BOS_up"] or struct["CHoCH_up"])
            if is_normal:
                if RELAX["ALLOW_NO_FVG_NORMAL"] and (fvg is not None) and fvg["type"]=="bear":
                    ok = False
            else:
                ok = ok and (fvg is None or fvg["type"]=="bull")
        else:
            ok = cross_dn and cond_rsi_impulse_dn and side_dn and (struct["BOS_down"] or struct["CHoCH_down"])
            if is_normal:
                if RELAX["ALLOW_NO_FVG_NORMAL"] and (fvg is not None) and fvg["type"]=="bull":
                    ok = False
            else:
                ok = ok and (fvg is None or fvg["type"]=="bear")
        return ok, {"struct":struct, "fvg":fvg, "price":float(last["close"]), "atr":float(last["ATR"])}

    ok5, extra = trigger_5m_relaxed(dfE, bias, is_normal=(MODE["current"]=="normal"))

    if hard_sniper:
        patt_ok_long  = bull_eng or two_bot  or (tri_ok and broke_up)
        patt_ok_short = bear_eng or two_top  or (tri_ok and broke_dn)
        if bias=="LONG":
            if not (ok5 and patt_ok_long): 
                notes.append("⚠️ Gatillo 5m NO ok (sniper)")
                return None
        else:
            if not (ok5 and patt_ok_short): 
                notes.append("⚠️ Gatillo 5m NO ok (sniper)")
                return None
    else:
        if not ok5:
            notes.append("⚠️ Gatillo 5m NO ok")
            return None
        else:
            notes.append("Gatillo 5m OK")

    # ========= TP / SL / RR =========
    price = float(dfE["close"].iloc[-1])
    atr_exec = float(dfE["ATR"].iloc[-1])
    sw1_lo, sw1_hi = df1["low"].tail(120).min(), df1["high"].tail(120).max()
    fib_ext_15 = fib_levels(sw15_lo, sw15_hi, is_long=is_long)["ext"]
    fib_ext_1h = fib_levels(sw1_lo,  sw1_hi,  is_long=is_long)["ext"]
    tp1, tp2, sl = sl_tp(price, is_long, sw5_lo, sw5_hi, sw15_lo, sw15_hi, atr_exec,
                         fvg=extra.get("fvg"), exts_15=fib_ext_15, exts_1h=fib_ext_1h)

    risk = abs(price - sl)
    reward1 = abs(tp1 - price)
    if risk <= 1e-9: 
        return None
    rr1 = reward1 / risk

    if rr1 < MIN_RR:
        notes.append(f"⚠️ RR {rr1:.2f} (Bajo)")
        if hard_sniper:
            return None 
    else:
        notes.append(f"RR {rr1:.2f} (Alto)")

    if extra.get("fvg"): 
        notes.append(f"FVG {extra['fvg']['type']}")
    st = extra.get("struct",{})
    if st.get("BOS_up"): notes.append("BOS↑")
    if st.get("BOS_down"): notes.append("BOS↓")
    if st.get("CHoCH_up"): notes.append("CHoCH↑")
    if st.get("CHoCH_down"): notes.append("CHoCH↓")

    # Confirmación textual (doble techo/suelo 15m/5m)
    two_top_15, two_bot_15, _, _ = double_top_bottom(df15, bars=40, tol=0.004)
    two_top_E,  two_bot_E,  _, _ = double_top_bottom(dfE,  bars=40, tol=0.004)

    patrones_detectados = []
    if is_long:
        if two_bot_15: patrones_detectados.append(mode_cfg["CONFIRM2"])
        if two_bot_E:  patrones_detectados.append(mode_cfg["EXECUTION"])
        if patrones_detectados:
            confirmacion_msg = f"Doble Suelo en {', '.join(patrones_detectados)}."
        else:
            confirmacion_msg = f"Sin patrón claro en {mode_cfg['CONFIRM2']}/{mode_cfg['EXECUTION']}."
    else:
        if two_top_15: patrones_detectados.append(mode_cfg["CONFIRM2"])
        if two_top_E:  patrones_detectados.append(mode_cfg["EXECUTION"])
        if patrones_detectados:
            confirmacion_msg = f"Doble Techo en {', '.join(patrones_detectados)}."
        else:
            confirmacion_msg = f"Sin patrón claro en {mode_cfg['CONFIRM2']}/{mode_cfg['EXECUTION']}."

    side = "LONG" if is_long else "SHORT"

    return {"side":side, "price":price, "sl":sl, "tp1":tp1, "tp2":tp2, 
            "notes":notes, 
            "zona_valor":zona_valor_reporte, 
            "confirmacion":confirmacion_msg}

# =========================
# Registro de señales + Gmail adjuntos
# =========================
DAILY_SIGNALS = []

def register_signal(d: dict):
    x = dict(d); x["ts"] = datetime.now(UTC).isoformat()
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
        [(f"signals_{day}.xlsx", buf.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
    )
    DAILY_SIGNALS.clear()

async def collect_candles_and_email():
    rows = []
    pairs = ACTIVE_PAIRS
    tfs   = TIMEFRAMES
    max_pairs = min(len(pairs), 20)
    for sym in pairs[:max_pairs]:
        for tf in tfs[:2]:
            df = fetch_ohlcv_first_ok(sym, tf, limit=120)
            if df.empty: continue
            last = df.tail(1).reset_index()
            last["symbol"] = sym; last["tf"] = tf
            rows.append(last)
    if not rows: return
    out = pd.concat(rows, ignore_index=True).rename(columns={"ts":"time"})
    csv_bytes = out.to_csv(index=False).encode("utf-8")
    stamp = datetime.now(UTC).strftime("%Y-%m-%d_%H%M")
    send_email_with_attachments(
        f"[{PROJECT_NAME}] Velas {stamp}",
        f"Adjunto CSV de última vela por símbolo/TF ({stamp} UTC).",
        [(f"candles_{stamp}.csv", csv_bytes, "text/csv")]
    )

# =========================
# ENTRENAMIENTO INICIAL (mensajes 2, 3, 4, 5)
# =========================
async def initial_training_cycle():
    TRAIN_APPROVED.clear()
    TRAIN_REJECTED.clear()

    # 2) Mensaje: descargando velas
    await send_tg("📥 Descargando velas para entrenamiento inicial...")

    sample_pairs = ACTIVE_PAIRS[:min(len(ACTIVE_PAIRS), TRAINING_SAMPLE)]

    # 3) Mensaje: entrenando estrategia
    await send_tg(
        f"🧠 Entrenando pares con tu estrategia monstruosa "
        f"(backtest rápido sobre {len(sample_pairs)} pares)..."
    )

    for sym in sample_pairs:
        for mk in CONFIG["ACTIVE_MODES"]:
            mode_cfg = CONFIG["MODES"][mk]
            try:
                # Entrenamiento: usar la misma lógica, pero sin modo sniper duro
                res = analyze_symbol(sym, mode_cfg, hard_sniper=False)
                if res:
                    TRAIN_APPROVED.append(sym)
                else:
                    TRAIN_REJECTED.append(sym)
            except Exception as e:
                print(f"⚠️ Error entrenando {sym}: {e}")
                TRAIN_REJECTED.append(sym)

    # 4) Mensaje: pares aprobados
    if TRAIN_APPROVED:
        uniq_approved = sorted(set(TRAIN_APPROVED))
        show = ", ".join(uniq_approved[:50])
        extra_note = ""
        if len(uniq_approved) > 50:
            extra_note = f"\n(Se muestran solo 50 de {len(uniq_approved)})."
        await send_tg(
            f"✅ PARES APROBADOS ({len(uniq_approved)}) en entrenamiento inicial:\n"
            f"{show}{extra_note}"
        )
    else:
        await send_tg("⚠️ Ningún par aprobó el filtro completo en el entrenamiento inicial. El bot igual seguirá buscando oportunidades en tiempo real.")

    # 5) Mensaje: pares rechazados
    if TRAIN_REJECTED:
        uniq_rejected = sorted(set(TRAIN_REJECTED))
        show_r = ", ".join(uniq_rejected[:50])
        extra_note_r = ""
        if len(uniq_rejected) > 50:
            extra_note_r = f"\n(Se muestran solo 50 de {len(uniq_rejected)})."
        await send_tg(
            f"❌ PARES RECHAZADOS ({len(uniq_rejected)}) en entrenamiento inicial:\n"
            f"{show_r}{extra_note_r}"
        )

# =========================
# Comandos Telegram
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MONITOR_ACTIVE
    MONITOR_ACTIVE = True
    modes = ", ".join([CONFIG["MODES"][m]["EXECUTION"] for m in CONFIG["ACTIVE_MODES"]])
    await send_tg(f"✅ Bot ACTIVADO\n🧭 Modo: <b>{MODE['current'].upper()}</b>\n🔎 Pares: <b>{len(ACTIVE_PAIRS)}</b>\n⏱️ {RUN_EVERY_SEC//60} min\nEjecuciones: {modes}")

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MONITOR_ACTIVE
    MONITOR_ACTIVE = False
    await send_tg("🛑 Bot DETENIDO.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    modes = ", ".join([CONFIG["MODES"][m]["EXECUTION"] for m in CONFIG["ACTIVE_MODES"]])
    await send_tg(f"📊 <b>ESTADO</b>\n• Pares: <b>{len(ACTIVE_PAIRS)}</b>\n• Modos: <b>{modes}</b>\n• Modo actual: <b>{MODE['current'].upper()}</b>\n• Monitoreo: <b>{'ACTIVO' if MONITOR_ACTIVE else 'DETENIDO'}</b>")

async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip().lower().split()
    if len(txt)>=2 and txt[1] in ("normal","sniper"):
        MODE["current"] = txt[1]
        await send_tg(f"⚙️ Modo cambiado a <b>{MODE['current'].upper()}</b>.")
    else:
        await send_tg("Usa: <code>/mode normal</code> o <code>/mode sniper</code>")

# =========================
# Bucles principales
# =========================
async def monitor_loop():
    while True:
        if not MONITOR_ACTIVE:
            await asyncio.sleep(3); continue
        hard_sniper_flag = (MODE["current"] == "sniper")
        for mk in CONFIG["ACTIVE_MODES"]:
            mode_cfg = CONFIG["MODES"][mk]
            print(f"[{datetime.now(UTC).strftime('%H:%M:%S')}] Escaneando {mk} (EXEC {mode_cfg['EXECUTION']})…")
            for sym in ACTIVE_PAIRS:
                try:
                    res = analyze_symbol(sym, mode_cfg, hard_sniper=hard_sniper_flag)
                    if not res:
                        continue

                    side = res["side"]
                    price, sl, tp1, tp2 = res["price"], res["sl"], res["tp1"], res["tp2"]
                    zona_valor, confirmacion = res["zona_valor"], res["confirmacion"]
                    notes = res["notes"]

                    if can_send(sym, side):
                        # 6) Mensaje de señal de bot (ya marcado en build_alert_v21)
                        msg = build_alert_v21(sym, side, price, sl, tp1, tp2, zona_valor, confirmacion, mode_cfg, notes)
                        await send_tg(msg)

                        full_notes = f"{zona_valor}|{confirmacion}|" + "|".join(notes)
                        register_signal({"symbol":sym,"side":side,"price":price,"sl":sl,"tp1":tp1,"tp2":tp2,"notes":full_notes,"mode":mk})
                        mark_sent(sym, side)

                except Exception as e:
                    print(f"⚠️ Error analizando {sym}: {e}")
        await asyncio.sleep(RUN_EVERY_SEC)

async def scheduler_loop():
    last_csv_min = -1
    while True:
        now = datetime.now(UTC)
        if now.minute in (0,30) and last_csv_min != now.minute:
            last_csv_min = now.minute
            try:
                await collect_candles_and_email()
            except Exception as e:
                print("collect_candles_and_email error:", e)
        if now.hour == 23 and now.minute == 59 and now.second < 5:
            try:
                await email_daily_signals_excel()
            except Exception as e:
                print("email_daily_signals_excel error:", e)
            await asyncio.sleep(5)
        await asyncio.sleep(1)

# =========================
# FastAPI keep-alive (/ping) + opcional /tv
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
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ Sin TELEGRAM_BOT_TOKEN, no se inicia bot de Telegram")
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

# =========================
# Entrypoint
# =========================
async def main_async():
    start_http()
    start_telegram_bot()

    # 1) Mensaje de inicio
    await _startup_notice()

    # 2–5) Descarga de velas + entrenamiento + aprobados/rechazados
    await initial_training_cycle()

    # 6) Ya queda corriendo el monitor y el scheduler
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
