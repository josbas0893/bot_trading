# ======================================================================
# Bot V4 – Render 24/7  (polars, modos dinámicos, filtros IA/reglas)
# - Multi-exchange (Kucoin/OKX...)
# - Sin python-telegram-bot (usa requests directo)
# - Sin Bitunix (solo /USDT de los exchanges disponibles)
# - Estrategia "semi-monstruosa":
#     * Bias 4h (EMA100)
#     * Trend 1h (EMA12/50/200)
#     * Setup 5m (EMA + RSI + rango + volumen)
#     * Swings + BOS/CHoCH
#     * FVG recientes como confluencia
# ======================================================================

import os, time, asyncio, threading
from datetime import datetime, UTC
from collections import defaultdict

import ccxt
import numpy as np
import polars as pl
import requests

from fastapi import FastAPI
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG ----------
EXCHANGES           = ["kucoin", "bybit", "binance", "okx"]   # fuente de datos
MAX_PAIRS           = int(os.getenv("MAX_PAIRS", "120"))
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
PROJECT_NAME        = os.getenv("PROJECT_NAME", "BotV4")
RUN_EVERY_SEC       = int(os.getenv("RUN_EVERY_SEC", "300"))
DATA_DIR            = "data"
BACKTEST_DAYS       = 60     # referencia, no filtro duro estricto

# ---------- MODOS DINÁMICOS ----------
MODOS = {
    "suave":  {"wr": 0.50, "trades": 3,  "desc": "Suave – más señales"},
    "normal": {"wr": 0.65, "trades": 10, "desc": "Normal – balance"},
    "sniper": {"wr": 0.80, "trades": 20, "desc": "Sniper – muy selectivo"}
}
MODE = {"current": "normal"}   # modo por defecto

# score mínimo por modo (1–7 puntos aprox.)
SCORE_MIN = {
    "suave":  3,
    "normal": 4,
    "sniper": 5,
}

MONITOR_ACTIVE = True
STATE = {
    "last_sent": {},                   # (symbol, side) -> timestamp
    "daily_count": defaultdict(int),   # (symbol, side, date) -> count
    "last_reset": datetime.now(UTC).date()
}
DAILY_SIGNALS = []

# ====================================================================
# EXCHANGES
# ====================================================================
EX_OBJS = {}
def init_exchanges():
    for name in EXCHANGES:
        try:
            klass = getattr(ccxt, name)
            opts = {"enableRateLimit": True}
            if name in ("bybit", "kucoin"):
                opts["options"] = {"defaultType": "future"}
            ex = klass(opts)
            ex.load_markets()
            EX_OBJS[name] = ex
            print(f"✅ {name} con {len(ex.symbols)} símbolos")
        except Exception as e:
            print(f"⚠️ No se pudo iniciar {name}: {e}")
init_exchanges()

# ====================================================================
# UTILS / TELEGRAM
# ====================================================================
def now_ts():
    return datetime.now(UTC)

async def send_tg(text: str):
    """
    Envío async a Telegram usando requests en un executor (no bloquea el loop).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[TG SIMULADO]\n{text}\n{'-'*40}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    def _post():
        try:
            r = requests.post(url, json=payload, timeout=15)
            if not r.ok:
                print(f"❌ Telegram {r.status_code}: {r.text}")
        except Exception as e:
            print(f"❌ Error enviando a Telegram: {e}")

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _post)

def ensure_daily_reset():
    """Resetea contadores diarios a medianoche UTC."""
    today = now_ts().date()
    if today != STATE["last_reset"]:
        STATE["last_reset"] = today
        STATE["daily_count"].clear()
        DAILY_SIGNALS.clear()
        print("🔄 Reset diario de contadores")

# ====================================================================
# DESCARGA DE VELAS (multi-exchange)
# ====================================================================
def csv_path(symbol, tf):
    return f"{DATA_DIR}/{tf}/{symbol.replace('/', '_')}_{tf}.csv"

def ensure_dirs():
    for tf in ["5m", "15m", "1h", "4h"]:
        os.makedirs(f"{DATA_DIR}/{tf}", exist_ok=True)

def download_once(symbol, tf, limit=1500):
    for ex in EX_OBJS.values():
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
            df = pl.DataFrame(
                data,
                schema=["ts", "open", "high", "low", "close", "volume"]
            )
            df = df.with_columns(pl.col("ts").cast(pl.Int64))
            df.write_csv(csv_path(symbol, tf))
            return df
        except Exception:
            continue
    return pl.DataFrame()

def load_or_download(symbol, tf):
    path = csv_path(symbol, tf)
    if os.path.exists(path):
        return pl.read_csv(path, dtypes={"ts": pl.Int64})
    return download_once(symbol, tf)

async def download_all_pares():
    ensure_dirs()
    pairs = build_pairs_list()
    total_rows = 0
    t0 = time.time()
    for sym in pairs:
        for tf in ["4h", "1h", "15m", "5m"]:
            df = download_once(sym, tf)
            total_rows += len(df)
    elapsed = time.time() - t0
    await send_tg(
        f"📥 Descarga inicial completa\n"
        f"🗂️  Pares: {len(pairs)}\n"
        f"📊 Filas: {total_rows/1e6:.1f} M\n"
        f"⏱️ Tiempo: {elapsed/60:.1f} min"
    )
    return pairs

# ====================================================================
# CONSTRUCCIÓN DE PARES (/USDT) – SIN BITUNIX, solo multi-exchange
# ====================================================================
def build_pairs_list():
    """
    Devuelve lista agregada de pares que terminen en /USDT de los exchanges
    disponibles (kucoin, okx, etc.) hasta MAX_PAIRS.
    """
    agg = []
    seen = set()

    for name, ex in EX_OBJS.items():
        try:
            syms = [s for s in ex.symbols if s.endswith("/USDT")]
            for s in syms:
                if s not in seen:
                    agg.append(s)
                    seen.add(s)
                if len(agg) >= MAX_PAIRS:
                    break
            if len(agg) >= MAX_PAIRS:
                break
        except Exception as e:
            print(f"⚠️ Error leyendo símbolos de {name}: {e}")

    agg = sorted(agg)[:MAX_PAIRS]
    print(f"📌 Pares compilados SIN Bitunix: {len(agg)} encontrados")
    return agg

# ====================================================================
# HELPERS DE VOLATILIDAD / SWINGS / FVG
# ====================================================================
def has_min_daily_range(df5: pl.DataFrame, min_pct: float = 0.015) -> bool:
    """
    Comprueba que el rango medio diario (high-low) sea al menos min_pct del precio medio.
    Evita pares que se mueven muy poquito (lateral aburrido).
    """
    if len(df5) < 100:
        return False
    window = 288
    df_last = df5 if len(df5) < window else df5.tail(window)
    mid_price = df_last["close"].mean()
    avg_range = (df_last["high"] - df_last["low"]).mean()
    if mid_price is None or avg_range is None:
        return False
    try:
        return float(avg_range) / float(mid_price) >= min_pct
    except ZeroDivisionError:
        return False

def detect_swings(df: pl.DataFrame, lookback: int = 2) -> pl.DataFrame:
    """
    Marca swings HH/LL simples. Trabaja sobre df (típicamente tail(200)).
    """
    n = df.height
    if n < lookback * 2 + 1:
        return df.with_columns(
            pl.lit(False).alias("is_swing_high"),
            pl.lit(False).alias("is_swing_low")
        )

    highs = df["high"].to_list()
    lows  = df["low"].to_list()
    sh = [False] * n
    sl = [False] * n

    for i in range(lookback, n - lookback):
        hi = highs[i]
        lo = lows[i]
        if all(hi > highs[j] for j in range(i - lookback, i + lookback + 1) if j != i):
            sh[i] = True
        if all(lo < lows[j] for j in range(i - lookback, i + lookback + 1) if j != i):
            sl[i] = True

    return df.with_columns(
        pl.Series("is_swing_high", sh),
        pl.Series("is_swing_low", sl)
    )

def get_bos_direction(df_sw: pl.DataFrame):
    """
    Detecta BOS/CHoCH simple basado en últimos dos swings.
    Devuelve 'bull', 'bear' o None.
    """
    if "is_swing_high" not in df_sw.columns or "is_swing_low" not in df_sw.columns:
        return None

    sh_idx = [i for i, v in enumerate(df_sw["is_swing_high"]) if v]
    sl_idx = [i for i, v in enumerate(df_sw["is_swing_low"]) if v]

    bos_bull = False
    bos_bear = False

    if len(sh_idx) >= 2:
        last, prev = sh_idx[-1], sh_idx[-2]
        if df_sw["high"][last] > df_sw["high"][prev]:
            bos_bull = True

    if len(sl_idx) >= 2:
        last, prev = sl_idx[-1], sl_idx[-2]
        if df_sw["low"][last] < df_sw["low"][prev]:
            bos_bear = True

    if bos_bull and not bos_bear:
        return "bull"
    if bos_bear and not bos_bull:
        return "bear"
    return None

def detect_recent_fvg(df: pl.DataFrame):
    """
    Detecta un FVG reciente (últimas ~8 velas).
    Devuelve dict {'side': 'LONG'/'SHORT', 'mid': precio_medio_gap} o None.
    Usamos FVG de 3 velas:
        Bullish: low[i] > high[i-2]
        Bearish: high[i] < low[i-2]
    """
    n = df.height
    if n < 3:
        return None

    highs = df["high"].to_list()
    lows  = df["low"].to_list()
    start = max(2, n - 8)

    for i in range(n - 1, start - 1, -1):
        hi2 = highs[i - 2]
        lo2 = lows[i - 2]
        hi  = highs[i]
        lo  = lows[i]

        # Bullish FVG
        if lo > hi2:
            mid = (lo + hi2) / 2.0
            return {"side": "LONG", "mid": mid}
        # Bearish FVG
        if hi < lo2:
            mid = (hi + lo2) / 2.0
            return {"side": "SHORT", "mid": mid}

    return None

# ====================================================================
# INDICADORES BÁSICOS (EMA/RSI) para cualquier TF
# ====================================================================
def compute_indicators(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        EMA_12=pl.col("close").ewm_mean(span=12),
        EMA_50=pl.col("close").ewm_mean(span=50),
        EMA_200=pl.col("close").ewm_mean(span=200),
    )
    delta = pl.col("close").diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    roll_up = gain.ewm_mean(alpha=1/14)
    roll_down = loss.ewm_mean(alpha=1/14)
    rs = roll_up / roll_down.replace(0, None)
    df = df.with_columns(RSI=100 - (100 / (1 + rs)))
    return df.drop_nulls()

# ====================================================================
# BACK-TEST INDIVIDUAL (ligero) – usa solo 5m + EMAs + RSI
# ====================================================================
def backtest_pare(symbol):
    df5 = load_or_download(symbol, "5m")
    if len(df5) < 500:
        return {"trades": 0, "wr": 0.0, "df": pl.DataFrame()}
    if not has_min_daily_range(df5, min_pct=0.015):
        return {"trades": 0, "wr": 0.0, "df": pl.DataFrame()}

    df5 = df5.with_columns(
        EMA_12=pl.col("close").ewm_mean(span=12),
        EMA_50=pl.col("close").ewm_mean(span=50),
        EMA_200=pl.col("close").ewm_mean(span=200),
    )
    delta = pl.col("close").diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    roll_up = gain.ewm_mean(alpha=1/14)
    roll_down = loss.ewm_mean(alpha=1/14)
    rs = roll_up / roll_down.replace(0, None)
    df5 = df5.with_columns(RSI=100 - (100 / (1 + rs)))

    trades = []
    for i in range(60, len(df5) - 20):
        c     = df5["close"][i]
        ema12 = df5["EMA_12"][i]
        ema50 = df5["EMA_50"][i]
        ema200= df5["EMA_200"][i]
        rsi   = df5["RSI"][i]
        if pl.is_nan(c):
            continue

        is_long = None
        # Condición base de estrategia (versión ligera)
        if c > ema200*0.998 and ema12 > ema50*0.998 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
            is_long = True
        elif c < ema200*1.002 and ema12 < ema50*1.002 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
            is_long = False

        if is_long is None:
            continue

        atr = (df5["high"][i] - df5["low"][i]).rolling_mean(14)[i]
        if pl.is_nan(atr):
            continue

        sl  = c - atr*1.8 if is_long else c + atr*1.8
        tp1 = c + atr*1.8*1.5 if is_long else c - atr*1.8*1.5

        fut = df5[i+1:i+21]
        if len(fut) < 2:
            continue

        if is_long:
            sl_hit = (fut["low"] <= sl).any()
            tp_hit = (fut["high"] >= tp1).any()
        else:
            sl_hit = (fut["high"] >= sl).any()
            tp_hit = (fut["low"]  <= tp1).any()

        if sl_hit and not tp_hit:
            trades.append(-1.0)
        elif tp_hit:
            trades.append(1.5)

    df_trades = pl.DataFrame({"R": trades}) if trades else pl.DataFrame({"R": []})
    wr = (df_trades["R"] > 0).mean() if len(df_trades) else 0.0
    return {"trades": len(df_trades), "wr": float(wr or 0.0), "df": df_trades}

# ====================================================================
# MENSAJE TELEGRAM: APROBADOS vs NO APROBADOS (entrenamiento)
# ====================================================================
async def filtra_aprobados():
    pairs = build_pairs_list()
    cfg_mode = MODOS[MODE["current"]]

    TRAIN_WR = 0.65
    TRAIN_TRADES_MIN = 3

    mayores_65 = []
    menores_65 = []
    aprobados_modo = []

    txt_hi = f"📚 <b>ENTRENAMIENTO COMPLETO</b>\n\n" \
             f"✅ Pares con WR ≥ {int(TRAIN_WR*100)}% (trades ≥ {TRAIN_TRADES_MIN}):\n"
    txt_lo = f"📚 <b>ENTRENAMIENTO COMPLETO</b>\n\n" \
             f"⚠️ Pares con WR < {int(TRAIN_WR*100)}% o trades < {TRAIN_TRADES_MIN}:\n"

    for sym in pairs:
        res = backtest_pare(sym)
        wr = res["wr"]
        tr = res["trades"]

        if wr >= TRAIN_WR and tr >= TRAIN_TRADES_MIN:
            mayores_65.append(sym)
            txt_hi += f"• {sym} – {wr:.0%} ({tr} trades)\n"
        else:
            menores_65.append(sym)
            txt_lo += f"• {sym} – {wr:.0%} ({tr} trades)\n"

        if wr >= cfg_mode["wr"] and tr >= cfg_mode["trades"]:
            aprobados_modo.append(sym)

    if mayores_65:
        await send_tg(txt_hi)
    if menores_65:
        await send_tg(txt_lo)

    await send_tg(
        f"💎 Aprobados para modo <b>{MODE['current'].upper()}</b>: "
        f"<b>{len(aprobados_modo)}</b> / {len(pairs)} pares\n"
        f"(WR mín {cfg_mode['wr']:.0%}, trades mín {cfg_mode['trades']})"
    )

    return aprobados_modo

# ====================================================================
# ANÁLISIS EN VIVO – Estrategia “semi-monstruosa”
# ====================================================================
APROBADOS = []

def analiza_vivo(symbol: str):
    # ---------- Timeframe 5m (ejecución) ----------
    df5 = load_or_download(symbol, "5m")
    if len(df5) < 120:
        return None
    if not has_min_daily_range(df5, min_pct=0.015):
        return None
    df5 = compute_indicators(df5)
    if len(df5) < 40:
        return None

    # ---------- Timeframe 1h (tendencia obligatoria) ----------
    df1h = load_or_download(symbol, "1h")
    if len(df1h) < 80:
        return None
    df1h = compute_indicators(df1h)
    last1h = df1h.tail(1)
    c1h     = last1h["close"][0]
    ema12_1h= last1h["EMA_12"][0]
    ema50_1h= last1h["EMA_50"][0]
    ema200_1h=last1h["EMA_200"][0]
    rsi1h   = last1h["RSI"][0]

    # ---------- Timeframe 4h (bias macro EMA100) ----------
    df4h = load_or_download(symbol, "4h")
    if len(df4h) < 40:
        return None
    df4h = df4h.with_columns(
        EMA_100=pl.col("close").ewm_mean(span=100)
    ).drop_nulls()
    last4h = df4h.tail(1)
    c4h    = last4h["close"][0]
    ema100_4h = last4h["EMA_100"][0]

    # ---------- Última vela 5m ----------
    last = df5.tail(1)
    c     = last["close"][0]
    ema12 = last["EMA_12"][0]
    ema50 = last["EMA_50"][0]
    ema200= last["EMA_200"][0]
    rsi   = last["RSI"][0]
    vol   = last["volume"][0]
    hi    = last["high"][0]
    lo    = last["low"][0]

    if any(pl.is_nan(x) for x in [c, ema12, ema50, ema200, rsi, vol, hi, lo]):
        return None

    # Filtro de volumen relativo (últimas 20 velas sin la actual)
    if len(df5) >= 21:
        vwin = df5.tail(21)
        v_mean = vwin["volume"][:-1].mean()
        if v_mean is not None and v_mean > 0 and vol < 1.5 * float(v_mean):
            return None

    # ---------- Score y dirección básica 5m ----------
    score = 0
    is_long = None

    # Condición base (como en backtest) – 5m
    if c > ema200*0.998 and ema12 > ema50*0.998 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
        is_long = True
        score += 2
    elif c < ema200*1.002 and ema12 < ema50*1.002 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
        is_long = False
        score += 2

    if is_long is None:
        return None

    # Rango diario ya filtrado → +1
    score += 1

    # RSI más fino (zona media) → +1
    if 40 <= rsi <= 60:
        score += 1

    # ---------- Bias macro 4h (EMA100) ----------
    bias_4h_long = c4h > ema100_4h
    bias_4h_short= c4h < ema100_4h

    if is_long and not bias_4h_long:
        return None
    if not is_long and not bias_4h_short:
        return None

    score += 1  # confluencia con bias 4h

    # ---------- Tendencia 1h (EMA 12/50/200) ----------
    trend_1h_bull = (ema12_1h > ema50_1h > ema200_1h) and rsi1h >= 40
    trend_1h_bear = (ema12_1h < ema50_1h < ema200_1h) and rsi1h <= 60

    if is_long and not trend_1h_bull:
        return None
    if not is_long and not trend_1h_bear:
        return None

    score += 1  # confluencia con 1h

    # ---------- Swings + BOS/CHoCH en 5m ----------
    df_sw = detect_swings(df5.tail(200), lookback=2)
    bos_dir = get_bos_direction(df_sw)
    if bos_dir == "bull" and is_long:
        score += 1
    elif bos_dir == "bear" and not is_long:
        score += 1

    # ---------- FVG reciente alineado ----------
    fvg = detect_recent_fvg(df5.tail(120))
    if fvg is not None:
        side_fvg = fvg["side"]
        mid_fvg  = fvg["mid"]
        dist = abs(c - mid_fvg) / max(c, 1e-9)
        if dist <= 0.008:  # precio cerca del centro del FVG (0.8 %)
            if side_fvg == "LONG" and is_long:
                score += 1
            elif side_fvg == "SHORT" and not is_long:
                score += 1

    # ---------- Validar score por modo ----------
    mode = MODE["current"]
    if score < SCORE_MIN.get(mode, 4):
        return None

    # ---------- SL/TP basados en rango actual ----------
    atr_like = hi - lo
    if pl.is_nan(atr_like) or atr_like <= 0:
        return None

    sl  = c - atr_like*1.8 if is_long else c + atr_like*1.8
    tp1 = c + atr_like*1.8*1.5 if is_long else c - atr_like*1.8*1.5
    tp2 = c + atr_like*1.8*2.5 if is_long else c - atr_like*1.8*2.5

    rr  = abs(tp1 - c) / max(abs(c - sl), 1e-9)
    if rr < 1.6:
        return None

    side = "LONG" if is_long else "SHORT"

    # Convertir score a estrellas máx 5
    stars = min(score, 7)
    star_5 = min(5, max(1, stars - 2))  # mapear 3–7 → 1–5

    return {
        "side": side,
        "price": float(c),
        "sl": float(sl),
        "tp1": float(tp1),
        "tp2": float(tp2),
        "rr": float(rr),
        "score": int(star_5)
    }

# ====================================================================
# ALERTA / REGISTRO
# ====================================================================
def fmt_price(x):
   
