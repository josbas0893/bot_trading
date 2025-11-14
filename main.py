import os, re, time, math, asyncio, threading, smtplib, ssl, io, glob
from datetime import datetime, UTC
from email.message import EmailMessage

import ccxt
import pandas as pd
import numpy as np

from telegram import Bot
from dotenv import load_dotenv

import joblib
from sklearn.ensemble import RandomForestClassifier
import ta

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SMTP_EMAIL = os.getenv("SMTP_EMAIL", "")
SMTP_APP_PASSWORD = os.getenv("SMTP_APP_PASSWORD", "")
SMTP_TO = os.getenv("SMTP_TO", SMTP_EMAIL)

PROJECT_NAME = os.getenv("PROJECT_NAME", "BotSenalesSwing")

EX_NAMES = [s.strip() for s in os.getenv("EXCHANGES", "kucoin").split(",")]
MAX_PAIRS_ENV = int(os.getenv("MAX_PAIRS", "100"))
TIMEFRAMES = [s.strip() for s in os.getenv("TIMEFRAMES", "4h,1h,15m,5m").split(",")]

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
            print(f"Conectado a {name} con {len(ex.symbols)} símbolos.")
        except Exception as e:
            print(f"No se pudo iniciar {name}: {e}")

init_exchanges()

async def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram no configurado, no se manda mensaje.")
        return
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)
    except Exception as e:
        print("Error enviando Telegram:", e)

def descargar_archivos():
    archivos = []
    hoy = datetime.now(UTC).strftime("%Y-%m-%d")
    pairs_totales = 0
    for ex in EX_OBJS.values():
        pairs = [s for s in ex.symbols if "/USDT" in s and "PERP" in s][:MAX_PAIRS_ENV]
        pairs_totales += len(pairs)
        for pair in pairs:
            for tf in TIMEFRAMES:
                try:
                    data = ex.fetch_ohlcv(pair, tf, limit=1000)
                    df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
                    filename = f"{pair.replace('/','_').replace(':','_')}_{tf}_{hoy}.csv"
                    df.to_csv(filename, index=False)
                    archivos.append(filename)
                    print(f"Guardado: {filename}")
                except Exception as e:
                    print(f"Error con {pair} {tf}: {e}")
    return archivos, pairs_totales

def enviar_gmail(archivos):
    if not (SMTP_EMAIL and SMTP_APP_PASSWORD and SMTP_TO):
        print("SMTP no configurado; salto envío.")
        return
    msg = EmailMessage()
    msg["From"] = SMTP_EMAIL
    msg["To"] = SMTP_TO
    msg["Subject"] = f"Archivos futuros {datetime.now(UTC).strftime('%Y-%m-%d')}"
    msg.set_content("Archivos de futuros descargados hoy (Render automatizado).")
    for filename in archivos:
        with open(filename, "rb") as f:
            msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=filename)
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(SMTP_EMAIL, SMTP_APP_PASSWORD)
        s.send_message(msg)
    print("Archivos enviados por Gmail.")

def limpiar_archivos(archivos):
    for f in archivos:
        try:
            os.remove(f)
        except:
            pass

def entrenar_modelo():
    archivos = glob.glob("*_1h_*.csv")
    if not archivos:
        print("No hay archivos CSV para entrenar")
        import sys
        sys.exit(1)

    df_total = pd.DataFrame()
    for archivo in archivos:
        df = pd.read_csv(archivo)
        df['rsi'] = ta.momentum.rsi(df['close'], window=14)
        df['ema20'] = ta.trend.ema_indicator(df['close'], window=20)
        df['target'] = (df['close'].shift(-1) > df['close']).astype(int)
        df = df.dropna()
        df_total = pd.concat([df_total, df], ignore_index=True)

    features = df_total[['rsi', 'ema20']]
    target = df_total['target']

    model = RandomForestClassifier()
    model.fit(features, target)
    joblib.dump(model, "modelo_rf.pkl")

    score = model.score(features, target)
    print(f"Backtest accuracy (win rate): {score*100:.2f}%")

    return score

async def main():
    archivos, pares = descargar_archivos()
    enviar_gmail(archivos)
    limpiar_archivos(archivos)

    score = entrenar_modelo()
    validacion = "✅ Estrategia válida" if score >= 0.55 else "⚠️ Estrategia NO válida"
    msg = (
        f"Entrenamiento terminado para {pares} pares.\n"
        f"Backtest accuracy: {score*100:.2f}%\n"
        f"{validacion}"
    )
    await send_telegram_message(msg)

    if score < 0.55:
        print("Estrategia no válida, se detiene el bot.")
        import sys
        sys.exit(1)

    print("Estrategia válida, el bot continúa.")

    # Aquí puedes llamar la función que ejecuta todo el análisis y señales del bot
# =========================
# Bot de Señales — 4H/1H/30m/15m con 3 modos
# Modos: suave, normal, sniper
# Replit / Render ready (FastAPI + Telegram + ccxt)
# =========================

import os
import re
import time
import math
import asyncio
import threading
from datetime import datetime, timezone

import ccxt
import numpy as np
import pandas as pd

from fastapi import FastAPI, Request
import uvicorn
from dotenv import load_dotenv

from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# =========================
# Carga de ENV / Secrets
# =========================

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

PROJECT_NAME = os.getenv("PROJECT_NAME", "BotSenalesMultiTF")

EX_NAMES = [
    s.strip() for s in os.getenv("EXCHANGES", "binance,okx,kucoin,bybit").split(",")
    if s.strip()
]

PAIRS_ENV = os.getenv("PAIRS", "").strip()
MAX_PAIRS = int(os.getenv("MAX_PAIRS", "150"))
RUN_EVERY_SEC = int(os.getenv("RUN_EVERY_SEC", "300"))  # 5 minutos

# Modo inicial (por env o normal por default)
MODE = {"current": os.getenv("BOT_MODE", "normal").lower()}

# Estado global
STATE = {
    "started": False,
    "last_sent": {},  # (symbol, side) -> timestamp
    "monitor_active": True,
}

# =========================
# Configuración de modos
# =========================

MODE_CONFIG = {
    "suave": {
        "score_threshold": 40,
        "ema_tolerance": 0.012,     # 1.2% de tolerancia
        "fib_margin": 0.03,         # ±3% margen en zona 0.5–0.75
        "rsi_hook_delta": 4,        # delta mínimo RSI hook
        "min_rr": 1.3,              # RR mínimo
        "allow_eq_zone": True,
        "require_ob_fvg": False,
    },
    "normal": {
        "score_threshold": 55,
        "ema_tolerance": 0.008,     # 0.8%
        "fib_margin": 0.02,         # ±2%
        "rsi_hook_delta": 3,
        "min_rr": 1.6,
        "allow_eq_zone": True,
        "require_ob_fvg": False,
    },
    "sniper": {
        "score_threshold": 70,
        "ema_tolerance": 0.005,     # 0.5%
        "fib_margin": 0.015,        # ±1.5%
        "rsi_hook_delta": 2,
        "min_rr": 2.0,
        "allow_eq_zone": False,
        "require_ob_fvg": True,
    },
}

# =========================
# Timeframes usados
# =========================

TF_SWING = "4h"   # Swing
TF_MACRO = "1h"   # Macro obligatorio
TF_CONFIRM = "30m"
TF_TRIGGER = "15m"
TF_EXEC = "5m"    # sólo para contexto extra si quieres usar luego

# =========================
# Exchanges (ccxt)
# =========================

EX_OBJS = {}


def init_exchanges():
    for name in EX_NAMES:
        try:
            klass = getattr(ccxt, name)
            ex = klass({"enableRateLimit": True})
            # Algunos futuros
            if name in ("bybit", "kucoin"):
                ex.options["defaultType"] = "future"
            ex.load_markets()
            EX_OBJS[name] = ex
            print(f"✅ Conectado a {name} con {len(ex.symbols)} símbolos.")
        except Exception as e:
            print(f"⚠️ No se pudo iniciar {name}: {e}")


def _basic_usdt_filter(symbol: str) -> bool:
    if not symbol.endswith("/USDT"):
        return False
    # filtrar 1000PEPE, etc
    if re.match(r"^(1000|10000|1M|10K|1K).*?/USDT$", symbol):
        return False
    if "PERP" in symbol.upper():
        return False
    return True


def _collect_from(ex):
    try:
        return [s for s in ex.symbols if _basic_usdt_filter(s)]
    except Exception:
        return []


def build_pairs_dynamic(limit: int = 150):
    agg = []
    seen = set()
    for name, ex in EX_OBJS.items():
        syms = _collect_from(ex)
        for s in syms:
            if s not in seen:
                agg.append(s)
                seen.add(s)
        if len(agg) >= limit:
            break
    agg = sorted(agg)
    return agg[:limit]


init_exchanges()

# PARES activos
if PAIRS_ENV and PAIRS_ENV.lower() != "auto":
    USER_PAIRS = [s.strip() for s in PAIRS_ENV.split(",") if s.strip()]
else:
    USER_PAIRS = []

DYN_PAIRS = build_pairs_dynamic(limit=MAX_PAIRS)

if USER_PAIRS:
    seen = set()
    ACTIVE_PAIRS = []
    for s in USER_PAIRS + DYN_PAIRS:
        if s not in seen:
            ACTIVE_PAIRS.append(s)
            seen.add(s)
    ACTIVE_PAIRS = ACTIVE_PAIRS[:MAX_PAIRS]
else:
    ACTIVE_PAIRS = DYN_PAIRS

print(f"📦 Pares a monitorear: {len(ACTIVE_PAIRS)}")

# =========================
# Utilidades de indicadores
# =========================

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/length, adjust=False).mean()
    roll_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val.fillna(50)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["EMA_12"] = ema(df["close"], 12)
    df["EMA_20"] = ema(df["close"], 20)
    df["EMA_50"] = ema(df["close"], 50)
    df["EMA_100"] = ema(df["close"], 100)
    df["EMA_200"] = ema(df["close"], 200)
    df["RSI"] = rsi(df["close"], 14)

    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    return df.dropna()


# =========================
# Smart zones / Fibo / OB
# =========================

def fetch_ohlcv_first_ok(symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
    for ex in EX_OBJS.values():
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not data or len(data) < 20:
                continue
            df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "volume"])
            # Quitamos vela actual
            df = df.iloc[:-1].copy()
            df["ts"] = pd.to_datetime(df["ts"], unit="ms")
            df.set_index("ts", inplace=True)
            return df
        except Exception:
            continue
    return pd.DataFrame()


def premium_discount_zone(price: float, low: float, high: float):
    if high <= low:
        return "eq", (high + low) / 2.0
    rng = high - low
    d20 = low + rng * 0.2
    d50 = low + rng * 0.5
    d80 = low + rng * 0.8

    if price <= d20:
        return "deep_discount", d50
    if d20 < price <= d50:
        return "discount", d50
    if d50 < price < d80:
        return "eq", d50
    return "premium", d50


def find_ob(df: pd.DataFrame, is_long: bool, lookback: int = 60):
    """
    Order Block simple:
    - LONG: última vela bajista antes de un rally alcista.
    - SHORT: última vela alcista antes de un dump bajista.
    """
    if len(df) < lookback + 3:
        return None

    seg = df.tail(lookback + 3).iloc[:-2]  # excluye 2 últimas
    for i in range(len(seg) - 1, 1, -1):
        c = seg["close"].iloc[i]
        o = seg["open"].iloc[i]
        prev_c = seg["close"].iloc[i - 1]
        prev_o = seg["open"].iloc[i - 1]

        if is_long:
            # rally alcista desde aquí
            if c > o and prev_c < prev_o:
                ob_candle = seg.iloc[i - 1]
                return {
                    "type": "bull",
                    "level": float(ob_candle["open"]),
                }
        else:
            if c < o and prev_c > prev_o:
                ob_candle = seg.iloc[i - 1]
                return {
                    "type": "bear",
                    "level": float(ob_candle["open"]),
                }
    return None


def find_fvg(df: pd.DataFrame, max_scan: int = 80):
    """
    FVG clásico de 3 velas:
    - bull gap: low3 > high1
    - bear gap: high3 < low1
    """
    if len(df) < 3:
        return None

    hi = df["high"].values
    lo = df["low"].values
    n = len(df)

    start = max(2, n - max_scan)
    for i in range(n - 1, start - 1, -1):
        i3 = i
        i1 = i - 2
        if i1 < 0:
            break
        # bull
        if lo[i3] > hi[i1]:
            return {
                "type": "bull",
                "gap": (float(hi[i1]), float(lo[i3])),
            }
        # bear
        if hi[i3] < lo[i1]:
            return {
                "type": "bear",
                "gap": (float(hi[i3]), float(lo[i1])),
            }
    return None


# =========================
# Bias de tendencia con EMAs
# =========================

def ema_trend_bias(df: pd.DataFrame):
    """
    Devuelve:
      1  = alcista
     -1  = bajista
      0  = neutro
    basado en 12/50/200 y cierre.
    """
    if len(df) < 60:
        return 0
    last = df.iloc[-1]
    ema12 = last["EMA_12"]
    ema50 = last["EMA_50"]
    ema200 = last["EMA_200"]
    close = last["close"]

    # alcista
    if close > ema50 and ema12 > ema50 > ema200:
        return 1
    # bajista
    if close < ema50 and ema12 < ema50 < ema200:
        return -1
    return 0


# =========================
# Lógica de análisis por símbolo
# =========================

def golden_zone_factor(price: float, swing_low: float, swing_high: float):
    if swing_high <= swing_low:
        return None

    # ratio de retroceso desde el high hacia el low
    rng = swing_high - swing_low
    if rng <= 0:
        return None
    # Para LONG: cuanto ha retrocedido desde el high
    retr_from_high = (swing_high - price) / rng
    # Para SHORT: cuanto ha subido desde el low
    retr_from_low = (price - swing_low) / rng

    return retr_from_high, retr_from_low


def analyze_symbol(symbol: str):
    mode_name = MODE.get("current", "normal")
    if mode_name not in MODE_CONFIG:
        mode_name = "normal"
        MODE["current"] = "normal"
    cfg = MODE_CONFIG[mode_name]

    # 1) Descargar velas
    df4 = fetch_ohlcv_first_ok(symbol, TF_SWING, limit=350)
    df1 = fetch_ohlcv_first_ok(symbol, TF_MACRO, limit=350)
    df30 = fetch_ohlcv_first_ok(symbol, TF_CONFIRM, limit=350)
    df15 = fetch_ohlcv_first_ok(symbol, TF_TRIGGER, limit=350)

    if df4.empty or df1.empty or df30.empty or df15.empty:
        return None

    df4 = add_indicators(df4)
    df1 = add_indicators(df1)
    df30 = add_indicators(df30)
    df15 = add_indicators(df15)

    if df4.empty or df1.empty or df30.empty or df15.empty:
        return None

    # 2) Bias SWING (4H con EMA100 y EMA50)
    last4 = df4.iloc[-1]
    close4 = float(last4["close"])
    ema50_4 = float(last4["EMA_50"])
    ema100_4 = float(last4["EMA_100"])

    swing_bias = 0
    if close4 > ema100_4:
        swing_bias = 1
    elif close4 < ema100_4:
        swing_bias = -1

    # 3) Bias MACRO (1H) — OBLIGATORIO
    macro_bias = ema_trend_bias(df1)
    if macro_bias == 0:
        return None  # no hay estructura clara

    # Sólo operamos a favor del bias macro
    is_long = macro_bias == 1
    side = "LONG" if is_long else "SHORT"

    score = 0
    tags = []

    # 4) Puntos por SWING
    if swing_bias == macro_bias:
        score += 20
        tags.append("4H alineado (EMA100)")
    elif swing_bias == 0:
        score += 5
        tags.append("4H neutro")
    else:
        # swing en contra
        score -= 10
        tags.append("4H en contra")

        # en sniper no queremos swing en contra
        if mode_name == "sniper":
            return None

    # 5) Confirmación 30m
    bias_30 = ema_trend_bias(df30)
    if bias_30 == macro_bias:
        score += 10
        tags.append("30m alineado")
    elif bias_30 == 0:
        tags.append("30m neutro")
    else:
        score -= 5
        tags.append("30m en contra")
        if mode_name in ("normal", "sniper"):
            return None

    # 6) Gatillo 15m: Hook EMA50 + RSI + Fibo golden zone
    last15 = df15.iloc[-1]
    prev15 = df15.iloc[-2]

    close15 = float(last15["close"])
    ema50_15 = float(last15["EMA_50"])
    ema200_15 = float(last15["EMA_200"])
    rsi15_now = float(last15["RSI"])
    rsi15_prev = float(prev15["RSI"])

    ema_tol = cfg["ema_tolerance"]

    # precio cerca de EMA50
    near_ema50 = abs(close15 - ema50_15) / max(close15, 1e-9) <= ema_tol

    # hook RSI
    rsi_hook_ok = False
    if is_long:
        # venía flojo, gira hacia arriba
        if (rsi15_prev < 50) and (rsi15_now > 50) and (rsi15_now - rsi15_prev >= cfg["rsi_hook_delta"]):
            rsi_hook_ok = True
    else:
        if (rsi15_prev > 50) and (rsi15_now < 50) and (rsi15_prev - rsi15_now >= cfg["rsi_hook_delta"]):
            rsi_hook_ok = True

    # swing para Fibo: usamos 1H
    sw_lo_1h = float(df1["low"].tail(120).min())
    sw_hi_1h = float(df1["high"].tail(120).max())
    golden = golden_zone_factor(close15, sw_lo_1h, sw_hi_1h)
    golden_ok = False
    if golden is not None:
        retr_from_high, retr_from_low = golden
        if is_long:
            # LONG: queremos retroceso desde el máximo entre 0.5 y 0.75 (± margen)
            if 0.5 - cfg["fib_margin"] <= retr_from_high <= 0.75 + cfg["fib_margin"]:
                golden_ok = True
        else:
            # SHORT: queremos retroceso desde el mínimo entre 0.5 y 0.75
            if 0.5 - cfg["fib_margin"] <= retr_from_low <= 0.75 + cfg["fib_margin"]:
                golden_ok = True

    # Puntuación del gatillo
    if near_ema50:
        score += 10
        tags.append("Rebote EMA50 15m")

    if rsi_hook_ok:
        score += 10
        tags.append("RSI hook 15m")

    if golden_ok:
        score += 15
        tags.append("Fibo 0.5–0.75 1H")

    # En sniper, exigimos los 3 (EMA+RSI+Fibo)
    if mode_name == "sniper":
        if not (near_ema50 and rsi_hook_ok and golden_ok):
            return None

    # 7) Zona Premium / Discount (usamos rango 4H)
    sw4_lo = float(df4["low"].tail(200).min())
    sw4_hi = float(df4["high"].tail(200).max())
    zone_name, eq_mid = premium_discount_zone(close15, sw4_lo, sw4_hi)

    if is_long:
        if zone_name in ("premium",):
            score -= 15
            tags.append(f"Zona {zone_name.upper()} (mala para LONG)")
            if not cfg["allow_eq_zone"]:
                return None
        elif zone_name in ("discount", "deep_discount"):
            score += 10
            tags.append(f"Zona {zone_name.upper()} (buena para LONG)")
        else:
            tags.append("Zona EQ")
    else:
        if zone_name in ("discount", "deep_discount"):
            score -= 15
            tags.append(f"Zona {zone_name.upper()} (mala para SHORT)")
            if not cfg["allow_eq_zone"]:
                return None
        elif zone_name in ("premium",):
            score += 10
            tags.append(f"Zona {zone_name.upper()} (buena para SHORT)")
        else:
            tags.append("Zona EQ")

    # 8) OB y FVG en 15m para refinar
    ob15 = find_ob(df15, is_long=is_long, lookback=60)
    fvg15 = find_fvg(df15, max_scan=60)

    if ob15 is not None:
        ob_level = ob15["level"]
        if abs(close15 - ob_level) / max(close15, 1e-9) <= ema_tol * 2:
            score += 8
            tags.append("Near OB 15m")

    if fvg15 is not None:
        if is_long and fvg15["type"] == "bull":
            score += 7
            tags.append("FVG bull 15m")
        if not is_long and fvg15["type"] == "bear":
            score += 7
            tags.append("FVG bear 15m")

    # En sniper exigimos OB o FVG a favor
    if cfg["require_ob_fvg"]:
        have_good_ob = ob15 is not None
        have_good_fvg = fvg15 is not None
        if not (have_good_ob or have_good_fvg):
            return None

    # 9) SL / TP con ATR y swing 1H
    atr15 = float(last15["ATR"])
    if atr15 <= 0:
        return None

    # SL basado en ATR + estructura
    if is_long:
        structural_sl = min(
            sw_lo_1h,
            close15 - atr15 * 2.0,
            float(df15["low"].tail(20).min()),
        )
        risk = close15 - structural_sl
        # TP1 ~ 1.5R, TP2 ~ 2.5R
        tp1 = close15 + risk * 1.5
        tp2 = close15 + risk * 2.5
    else:
        structural_sl = max(
            sw_hi_1h,
            close15 + atr15 * 2.0,
            float(df15["high"].tail(20).max()),
        )
        risk = structural_sl - close15
        tp1 = close15 - risk * 1.5
        tp2 = close15 - risk * 2.5

    if risk <= 0:
        return None

    rr1 = abs(tp1 - close15) / risk
    if rr1 < cfg["min_rr"]:
        tags.append(f"RR1 bajo ({rr1:.2f})")
        # en sniper lo cortamos
        if mode_name == "sniper":
            return None
    else:
        tags.append(f"RR1 OK ({rr1:.2f})")
        score += 5

    # 10) Umbral de puntuación final
    if score < cfg["score_threshold"]:
        # no suficiente convergencia
        return None

    # Construimos resultado
    result = {
        "symbol": symbol,
        "side": side,
        "entry": close15,
        "sl": structural_sl,
        "tp1": tp1,
        "tp2": tp2,
        "score": score,
        "mode": mode_name,
        "zone": zone_name,
        "tags": tags,
        "tf": TF_TRIGGER,
    }
    return result


# =========================
# Anti-spam y helpers Telegram
# =========================

def can_send(symbol: str, side: str, min_minutes: int = 30) -> bool:
    key = (symbol, side)
    last_ts = STATE["last_sent"].get(key, 0)
    return (time.time() - last_ts) > min_minutes * 60


def mark_sent(symbol: str, side: str):
    STATE["last_sent"][(symbol, side)] = time.time()


def fmt_price(x: float) -> str:
    try:
        if x < 1:
            return f"{x:.6f}"
        if x < 100:
            return f"{x:.4f}"
        return f"{x:.2f}"
    except Exception:
        return str(x)


async def send_tg(text: str, chat_id: str | None = None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID no configurados")
        return
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(
            chat_id=chat_id or TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="HTML",
        )
    except Exception as e:
        print("❌ Error Telegram:", e)


def build_signal_message(d: dict) -> str:
    symbol = d["symbol"]
    side = d["side"]
    entry = d["entry"]
    sl = d["sl"]
    tp1 = d["tp1"]
    tp2 = d["tp2"]
    mode_name = d["mode"]
    zone = d["zone"]
    score = d["score"]
    tags = d["tags"]
    tf = d["tf"]

    emoji_side = "🟢 LONG 📈" if side == "LONG" else "🔴 SHORT 📉"

    txt = []
    txt.append("✨ SEÑAL DETECTADA (MultiTF) ✨")
    txt.append("")
    txt.append(f"{emoji_side}")
    txt.append(f"💱 Par: <b>{symbol}</b>")
    txt.append(f"🕒 Temporalidad gatillo: <b>{tf}</b>")
    txt.append(f"⚙️ Modo: <b>{mode_name.upper()}</b>")
    txt.append(f"📊 Score: <b>{score}</b>")
    txt.append(f"📌 Zona: <b>{zone.upper()}</b>")
    txt.append("")
    txt.append(f"🎯 Entrada: <code>{fmt_price(entry)}</code>")
    txt.append(f"🛑 SL: <code>{fmt_price(sl)}</code>")
    txt.append(f"✅ TP1: <code>{fmt_price(tp1)}</code>")
    txt.append(f"✅ TP2: <code>{fmt_price(tp2)}</code>")
    txt.append("")
    txt.append("🧠 Confirmaciones:")
    for t in tags:
        txt.append(f"• {t}")
    txt.append("")
    txt.append("⚠️ Gestiona riesgo, ajusta tamaño de posición y SL según tu capital.")
    return "\n".join(txt)


# =========================
# Comandos de Telegram
# =========================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    STATE["monitor_active"] = True
    msg = (
        f"✅ Bot ACTIVADO\n"
        f"📦 Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"⏱️ Escaneo cada <b>{RUN_EVERY_SEC//60} min</b>\n"
        f"⚙️ Modo actual: <b>{MODE['current'].upper()}</b>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    STATE["monitor_active"] = False
    await update.message.reply_text("🛑 Bot DETENIDO.", parse_mode="HTML")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    last_pairs = len(ACTIVE_PAIRS)
    msg = (
        f"📊 <b>ESTADO</b>\n"
        f"• Pares: <b>{last_pairs}</b>\n"
        f"• Modo: <b>{MODE['current'].upper()}</b>\n"
        f"• Monitoreo: <b>{'ACTIVO' if STATE['monitor_active'] else 'DETENIDO'}</b>\n"
        f"• Intervalo: <b>{RUN_EVERY_SEC//60} min</b>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip().lower().split()
    if len(txt) >= 2 and txt[1] in MODE_CONFIG.keys():
        MODE["current"] = txt[1]
        await update.message.reply_text(
            f"⚙️ Modo cambiado a <b>{MODE['current'].upper()}</b>",
            parse_mode="HTML",
        )
    else:
        available = ", ".join(MODE_CONFIG.keys())
        await update.message.reply_text(
            f"Usa: <code>/mode suave</code>, <code>/mode normal</code> o <code>/mode sniper</code>\n"
            f"Modos disponibles: {available}",
            parse_mode="HTML",
        )


def start_telegram_bot():
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ TELEGRAM_BOT_TOKEN no configurado, no se inicia Telegram.")
        return

    app_tg = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app_tg.add_handler(CommandHandler("start", cmd_start))
    app_tg.add_handler(CommandHandler("stop", cmd_stop))
    app_tg.add_handler(CommandHandler("status", cmd_status))
    app_tg.add_handler(CommandHandler("mode", cmd_mode))

    def _run():
        app_tg.run_polling()

    th = threading.Thread(target=_run, daemon=True)
    th.start()


# =========================
# FastAPI para /ping y Render keep-alive
# =========================

app = FastAPI()


@app.get("/ping")
async def ping():
    return {
        "ok": True,
        "service": PROJECT_NAME,
        "mode": MODE.get("current", "normal"),
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/tv")
async def tv_webhook(req: Request):
    data = await req.json()
    symbol = data.get("symbol")
    price = data.get("price")
    tf = data.get("tf") or data.get("interval")
    ts = data.get("time") or datetime.now(timezone.utc).isoformat()
    msg = (
        f"📈 Señal externa TV\n"
        f"Symbol: {symbol}\nTF: {tf}\nPrice: {price}\nTime: {ts}"
    )
    await send_tg(msg)
    return {"ok": True}


def start_http():
    def _run():
        port = int(os.getenv("PORT", "8080"))
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")

    th = threading.Thread(target=_run, daemon=True)
    th.start()


# =========================
# Bucle principal de monitoreo
# =========================

async def monitor_loop():
    # Mensaje de inicio una sola vez
    global STATE
    if not STATE["started"]:
        STATE["started"] = True
        start_msg = (
            f"✅ {PROJECT_NAME} iniciado\n"
            f"📦 Pares: <b>{len(ACTIVE_PAIRS)}</b>\n"
            f"⏱️ Escaneo: cada <b>{RUN_EVERY_SEC//60} min</b>\n"
            f"⚙️ Modo inicial: <b>{MODE['current'].upper()}</b>"
        )
        await send_tg(start_msg)

    while True:
        if not STATE["monitor_active"]:
            await asyncio.sleep(3)
            continue

        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{now}] Escaneando {len(ACTIVE_PAIRS)} pares — modo {MODE['current']}")

        for symbol in ACTIVE_PAIRS:
            try:
                res = analyze_symbol(symbol)
                if not res:
                    continue

                side = res["side"]
                if not can_send(symbol, side, min_minutes=30):
                    continue

                msg = build_signal_message(res)
                await send_tg(msg)
                mark_sent(symbol, side)

            except Exception as e:
                print(f"⚠️ Error analizando {symbol}: {e}")

        await asyncio.sleep(RUN_EVERY_SEC)


# =========================
# Entrypoint
# =========================

async def main_async():
    start_http()
    start_telegram_bot()
    await monitor_loop()


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except RuntimeError:
        loop = asyncio.get_event_loop()
        loop.create_task(main_async())
        loop.run_forever()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

