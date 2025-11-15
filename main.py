# ============================================================
# Bot de Señales MultiTF — Render Ready
# Swing 4h · Macro 1h · Confirm 30m · Gatillo 15m · Exec 5m
# 3 modos: suave / normal / sniper
# Señales con contexto tipo "bot pro"
# ============================================================

import os
import re
import time
import asyncio
import threading
import smtplib
import ssl
import io
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import ccxt
import numpy as np
import pandas as pd
import requests

from fastapi import FastAPI
import uvicorn
from dotenv import load_dotenv

# ============================================================
# ENV / Secrets
# ============================================================

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SMTP_EMAIL        = os.getenv("SMTP_EMAIL", "").strip()
SMTP_APP_PASSWORD = os.getenv("SMTP_APP_PASSWORD", "").strip()
SMTP_TO           = os.getenv("SMTP_TO", SMTP_EMAIL).strip()

PROJECT_NAME = os.getenv("PROJECT_NAME", "BotSenalesV3")

EX_NAMES       = [s.strip() for s in os.getenv("EXCHANGES", "okx,kucoin").split(",") if s.strip()]
PAIRS_ENV      = os.getenv("PAIRS", "").strip()
MAX_PAIRS_ENV  = int(os.getenv("MAX_PAIRS", "150"))
MODE_DEFAULT   = os.getenv("MODE_DEFAULT", "normal").strip().lower()
SHIT_FILTER    = os.getenv("SHIT_FILTER", "false").strip().lower() == "true"

RUN_EVERY_SEC  = 300  # escaneo cada 5 min

# ============================================================
# Config general de modos
# ============================================================

MODE = {"current": MODE_DEFAULT if MODE_DEFAULT in ("suave", "normal", "sniper") else "normal"}
MONITOR_ACTIVE = True

MODE_CFG = {
    "suave": {
        "pb_min": 0.25,
        "pb_max": 0.85,
        "rr_min": 1.2,
        "hook_eps": 0.010,  # tolerancia hook EMA50 15m
    },
    "normal": {
        "pb_min": 0.35,
        "pb_max": 0.80,
        "rr_min": 1.4,
        "hook_eps": 0.007,
    },
    "sniper": {
        "pb_min": 0.45,
        "pb_max": 0.75,
        "rr_min": 1.8,
        "hook_eps": 0.005,
    },
}

# EMA / RSI / ATR config
EMA_SWING_4H  = 100  # Swing 4h
EMA_MACRO_1H  = (12, 50, 200)
EMA_CONF_30M  = (12, 50, 200)
EMA_TRIG_15M  = (20, 50, 200)
EMA_EXEC_5M   = (20, 50, 200)
RSI_PERIOD    = 14
ATR_PERIOD    = 14

STATE = {
    "started": False,
    "last_sent": {},      # (symbol, side) -> timestamp
    "tg_update_offset": 0 # para getUpdates
}

DAILY_SIGNALS = []

# ============================================================
# Exchanges (ccxt)
# ============================================================

EX_OBJS = {}

def init_exchanges():
    for name in EX_NAMES:
        try:
            klass = getattr(ccxt, name)
            opts = {"enableRateLimit": True}
            # Si quisieras futuros, aquí configuras defaultType
            ex = klass(opts)
            ex.load_markets()
            EX_OBJS[name] = ex
            print(f"✅ Conectado a {name} con {len(ex.symbols)} símbolos.")
        except Exception as e:
            print(f"⚠️ No se pudo iniciar {name}: {e}")

def _basic_usdt_filter(symbol: str) -> bool:
    # Filtro básico USDT spot (sin PERP, sin cosas raras)
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

# ============================================================
# PARES activos (PAIRS env + auto)
# ============================================================

USER_PAIRS = []
if PAIRS_ENV and PAIRS_ENV.lower() != "auto":
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

# SHIT_FILTER solo aplica en modo auto (sin USER_PAIRS)
if SHIT_FILTER and not USER_PAIRS:
    # Filtro simple: dejar solo nombres "serios" (base <= 5 chars o conocidos)
    BLUECHIPS = {
        "BTC","ETH","SOL","BNB","AVAX","INJ","ADA","XRP","DOGE",
        "OP","ARB","LTC","LINK","DOT","MATIC","SUI","APT","TIA",
        "INJ","ATOM","RUNE","NEAR","SEI","DYDX","PYTH","JUP",
    }
    filtered = []
    for sym in ACTIVE_PAIRS:
        base = sym.split("/")[0]
        if len(base) <= 5 or base in BLUECHIPS:
            filtered.append(sym)
    print(f"🧹 SHIT_FILTER activado: {len(filtered)}/{len(ACTIVE_PAIRS)} pares.")
    ACTIVE_PAIRS = filtered

print(f"📦 Pares a monitorear: {len(ACTIVE_PAIRS)}")

# ============================================================
# Telegram helpers (sin python-telegram-bot)
# ============================================================

TG_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""

def send_tg_sync(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("⚠️ Telegram no configurado, no se envía mensaje.")
        return
    try:
        resp = requests.post(
            TG_API_BASE + "/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML"
            },
            timeout=10
        )
        if resp.status_code != 200:
            print("❌ Telegram error:", resp.status_code, resp.text)
    except Exception as e:
        print("❌ Telegram exception:", e)

async def send_tg(text: str):
    # wrapper async
    await asyncio.to_thread(send_tg_sync, text)

async def startup_notice():
    await send_tg(
        f"✅ <b>{PROJECT_NAME}</b> iniciado\n"
        f"🧭 Modo: <b>{MODE['current'].upper()}</b>\n"
        f"📦 Pares activos: <b>{len(ACTIVE_PAIRS)}</b>\n"
        f"⏱️ Escaneo cada <b>{RUN_EVERY_SEC//60} min</b>\n"
        f"🧠 Estrategia: Swing 4h · Macro 1h · Confirm 30m · Gatillo 15m · Exec 5m"
    )

def can_send(symbol: str, side: str) -> bool:
    key = (symbol, side)
    t = STATE["last_sent"].get(key, 0.0)
    return (time.time() - t) > (30 * 60)  # 30 minutos

def mark_sent(symbol: str, side: str):
    STATE["last_sent"][(symbol, side)] = time.time()

# ============================================================
# Indicadores básicos (EMA, RSI, ATR)
# ============================================================

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    roll_up = gain.ewm(alpha=1/length, adjust=False).mean()
    roll_down = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)

def compute_indicators(df: pd.DataFrame, ema_set: tuple[int, ...]) -> pd.DataFrame:
    if df.empty:
        return df
    for length in ema_set:
        df[f"EMA_{length}"] = ema(df["close"], length)
    df["RSI"] = rsi(df["close"], RSI_PERIOD)
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(ATR_PERIOD).mean()
    return df.dropna()

# ============================================================
# Fetch OHLCV
# ============================================================

def fetch_ohlcv_first_ok(symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
    for _, ex in EX_OBJS.items():
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if data and len(data) >= 50:
                df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
                # quitar vela actual en formación
                df = df.iloc[:-1].copy()
                df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                df.set_index("ts", inplace=True)
                return df
        except Exception:
            continue
    return pd.DataFrame()

# ============================================================
# Estructura HH/HL / LL/LH con fractales
# ============================================================

def find_swings(df: pd.DataFrame, lookback: int = 2):
    if len(df) < 2 * lookback + 3:
        return [], []
    highs = df["high"].values
    lows = df["low"].values
    idx_list = list(df.index)

    swing_highs = []
    swing_lows  = []

    for i in range(lookback, len(df) - lookback):
        window_h = highs[i-lookback:i+lookback+1]
        window_l = lows[i-lookback:i+lookback+1]
        if highs[i] == window_h.max():
            swing_highs.append((idx_list[i], highs[i]))
        if lows[i] == window_l.min():
            swing_lows.append((idx_list[i], lows[i]))

    return swing_highs, swing_lows

def detect_structure(df: pd.DataFrame, side: str):
    """
    Devuelve:
      - structure_label: 'LL/LH', 'HH/HL' o '' si no claro
      - flag_ok: True/False si está alineado con side
      - pivot_res/soporte cercano: bool, text
    """
    sw_hi, sw_lo = find_swings(df, lookback=2)
    structure_label = ""
    aligned = False
    pivot_label = ""
    pivot_near = False

    if len(sw_hi) >= 2 and len(sw_lo) >= 2:
        # últimos 2 swings relevantes
        _, hi1 = sw_hi[-2]
        _, hi2 = sw_hi[-1]
        _, lo1 = sw_lo[-2]
        _, lo2 = sw_lo[-1]

        if hi2 < hi1 and lo2 < lo1:
            structure_label = "LL/LH"
            aligned = (side == "SHORT")
        elif hi2 > hi1 and lo2 > lo1:
            structure_label = "HH/HL"
            aligned = (side == "LONG")

    # Pivote fractal cercano (resistencia/soporte)
    last_price = float(df["close"].iloc[-1])
    # tomar swing más reciente relevante
    if sw_hi:
        hi_pivot = sw_hi[-1][1]
        dist = abs(last_price - hi_pivot) / max(hi_pivot, 1e-9)
        if dist <= 0.005:  # 0.5%
            pivot_near = True
            pivot_label = "Cerca de pivote fractal (resistencia)"
    if not pivot_near and sw_lo:
        lo_pivot = sw_lo[-1][1]
        dist = abs(last_price - lo_pivot) / max(lo_pivot, 1e-9)
        if dist <= 0.005:
            pivot_near = True
            pivot_label = "Cerca de pivote fractal (soporte)"

    return structure_label, aligned, pivot_near, pivot_label

# ============================================================
# Momentum tipo "5 de 9 velas"
# ============================================================

def momentum_bars(df: pd.DataFrame, side: str, bars: int = 9):
    seg = df.tail(bars)
    ups = (seg["close"] > seg["open"]).sum()
    downs = (seg["close"] < seg["open"]).sum()

    if side == "LONG":
        dominant = ups
        label_side = "alcista"
    else:
        dominant = downs
        label_side = "bajista"

    strength = "débil"
    if dominant >= 7:
        strength = "fuerte"
    elif dominant >= 5:
        strength = "moderado"

    label = f"Momentum {label_side} {strength} ({downs if side=='SHORT' else ups}/{bars} {'bajistas' if side=='SHORT' else 'alcistas'})"
    return dominant, label

# ============================================================
# EMA20 distance + VWAP
# ============================================================

def ema20_distance(df: pd.DataFrame):
    if f"EMA_{20}" not in df.columns:
        return None, ""
    close = float(df["close"].iloc[-1])
    ema20 = float(df["EMA_20"].iloc[-1])
    dist = (close - ema20) / max(ema20, 1e-9)
    label = f"Zona {'cerca' if abs(dist) <= 0.005 else ''} ({dist*100:.2f}% de EMA20)"
    return dist, label

def compute_vwap(df: pd.DataFrame, bars: int = 96):
    seg = df.tail(bars)
    if seg["volume"].sum() <= 0:
        return None
    typical = (seg["high"] + seg["low"] + seg["close"]) / 3.0
    vwap = (typical * seg["volume"]).sum() / seg["volume"].sum()
    return float(vwap)

def vwap_context(df: pd.DataFrame, side: str):
    vwap = compute_vwap(df, bars=96)
    if vwap is None:
        return ""
    close = float(df["close"].iloc[-1])
    dist = (close - vwap) / max(vwap, 1e-9)
    if abs(dist) <= 0.005:
        if side == "SHORT" and close > vwap:
            return "Cerca de VWAP (resistencia institucional)"
        if side == "LONG" and close < vwap:
            return "Cerca de VWAP (soporte institucional)"
        return "Cerca de VWAP"
    return ""

# ============================================================
# Sesiones (en CST aproximado)
# ============================================================

def session_from_ts(ts_utc: pd.Timestamp):
    # Render corre en UTC; convertimos a CST (UTC-5 approx fijo)
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)
    ts_cst = ts_utc - timedelta(hours=5)
    h = ts_cst.hour

    if 0 <= h < 7:
        ses = "Asia"
    elif 7 <= h < 13:
        ses = "Londres"
    elif 13 <= h < 20:
        ses = "USA"
    else:
        ses = "Fuera de sesión principal"

    ts_str = ts_cst.strftime("%d %b %H:%M CST")
    return ses, ts_str

# ============================================================
# Pullback / Hook / RR / SL-TP
# ============================================================

def pullback_ratio(df: pd.DataFrame, side: str, lookbars: int = 80):
    seg = df.tail(lookbars)
    hi = float(seg["high"].max())
    lo = float(seg["low"].min())
    close = float(seg["close"].iloc[-1])
    rng = max(hi - lo, 1e-9)
    if side == "LONG":
        # qué tanto ha retrocedido desde el máximo
        return (hi - close) / rng
    else:
        # qué tanto ha rebotado desde el mínimo
        return (close - lo) / rng

def hook_ema50_rsi(df: pd.DataFrame, side: str, eps: float):
    if len(df) < 3 or "EMA_50" not in df.columns:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(last["close"])
    ema50 = float(last["EMA_50"])
    rsi_now = float(last["RSI"])
    rsi_prev = float(prev["RSI"])

    dist = abs(close - ema50) / max(close, 1e-9)
    near = dist <= eps

    if side == "LONG":
        # RSI girando hacia arriba cerca de EMA50
        rsi_turn = (rsi_prev <= 40 and rsi_now > rsi_prev)
    else:
        # RSI girando hacia abajo cerca de EMA50
        rsi_turn = (rsi_prev >= 60 and rsi_now < rsi_prev)

    return near and rsi_turn

def rr_sl_tp(price: float, atr_val: float, df_15: pd.DataFrame, side: str, mode_name: str):
    cfg = MODE_CFG[mode_name]
    rr_min = cfg["rr_min"]

    seg = df_15.tail(80)
    sw_hi = float(seg["high"].max())
    sw_lo = float(seg["low"].min())

    if side == "LONG":
        sl = min(sw_lo, price - 1.5 * atr_val)
        risk = price - sl
        tp1 = price + max(rr_min * risk * 0.8, atr_val * 1.5)
        tp2 = price + max(rr_min * risk * 1.2, atr_val * 2.0)
    else:
        sl = max(sw_hi, price + 1.5 * atr_val)
        risk = sl - price
        tp1 = price - max(rr_min * risk * 0.8, atr_val * 1.5)
        tp2 = price - max(rr_min * risk * 1.2, atr_val * 2.0)

    risk = max(abs(price - sl), 1e-9)
    reward1 = abs(tp1 - price)
    rr1 = reward1 / risk
    return float(sl), float(tp1), float(tp2), float(rr1)

def fmt_price(x: float) -> str:
    try:
        if x < 0.001:
            return f"{x:.8f}"
        if x < 1:
            return f"{x:.6f}"
        if x < 100:
            return f"{x:.4f}"
        return f"{x:.2f}"
    except Exception:
        return str(x)

# ============================================================
# SCORE / Mensaje tipo bot "pro"
# ============================================================

def build_signal_message(symbol: str,
                         side: str,
                         tf_exec: str,
                         price: float,
                         sl: float,
                         tp1: float,
                         tp2: float,
                         score_v1: int,
                         score_v1_max: int,
                         score_v2_raw: int,
                         gates_passed: int,
                         ema20_label: str,
                         momentum_label: str,
                         structure_label: str,
                         structure_ok: bool,
                         vwap_label: str,
                         pivot_label: str,
                         session_name: str,
                         ts_str: str):
    # Mapear scores
    v1_pct = score_v1 / max(score_v1_max, 1) if score_v1_max > 0 else 0.0
    v2_pct = score_v2_raw / 100.0

    entry_score_100 = int(round((v1_pct * 0.6 + v2_pct * 0.4) * 100))
    confianza_raw = v1_pct * 70 + v2_pct * 80
    confianza = int(min(150, round(confianza_raw)))

    score_v2_scaled = int(round(score_v2_raw * 1.3))  # 0–130 aprox

    # Texto calidad entrada
    if entry_score_100 >= 80:
        entry_label = "EXCELENTE"
    elif entry_score_100 >= 65:
        entry_label = "BUENA"
    elif entry_score_100 >= 50:
        entry_label = "DECENTE"
    else:
        entry_label = "RIESGOSA"

    # Direccional
    if side == "SHORT":
        title = "🔻 SHORT ALERT"
        emoji_side = "🔴 SHORT"
    else:
        title = "🟢 LONG ALERT"
        emoji_side = "🟢 LONG"

    # Porcentajes vs entrada
    tp1_pct = (tp1 - price) / price * 100.0
    tp2_pct = (tp2 - price) / price * 100.0
    sl_pct  = (sl  - price) / price * 100.0

    # Momentum / estructura
    struct_txt = ""
    if structure_label:
        struct_txt = f"{structure_label} {'confirmado' if structure_ok else '(no alineado)'}"

    context_parts = []
    if struct_txt:
        context_parts.append(struct_txt)
    if momentum_label:
        context_parts.append(momentum_label)
    if ema20_label:
        context_parts.append(ema20_label)
    if vwap_label:
        context_parts.append(vwap_label)
    if pivot_label:
        context_parts.append(pivot_label)
    if session_name:
        context_parts.append(f"Sesión {session_name}")

    contexto_line = " | ".join(context_parts) if context_parts else "Sin contexto extra"

    msg = (
        f"{title}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 <b>{symbol}</b>\n"
        f"📊 TF Exec: <b>{tf_exec}</b>\n"
        f"⏰ {ts_str}\n\n"
        f"{emoji_side}\n"
        f"💵 Entrada: <b>${fmt_price(price)}</b>\n"
        f"🟢 Entrada {entry_label} ({entry_score_100}/100)\n\n"
        f"🎯 Confianza: <b>{confianza}/150</b>\n"
        f"📊 Score V1: <b>{score_v1}/{score_v1_max}</b> ({v1_pct*100:.0f}%)\n"
        f"⚡ Score V2: <b>{score_v2_scaled}/130</b> | ✅ Gates: {gates_passed}/8\n\n"
        f"📈 TARGETS (aprox. sin apalancamiento):\n"
        f"🎯 TP1: <b>${fmt_price(tp1)}</b> ({tp1_pct:+.2f}%)\n"
        f"🎯 TP2: <b>${fmt_price(tp2)}</b> ({tp2_pct:+.2f}%)\n"
        f"🛑 SL:  <b>${fmt_price(sl)}</b> ({sl_pct:+.2f}%)\n\n"
        f"📊 CONTEXTO:\n"
        f"{contexto_line}\n\n"
        f"⚠️ Gestión sugerida:\n"
        f"- Tomar parcial en TP1 y mover SL a BE.\n"
        f"- Ajustar tamaño de posición según tu riesgo (1–2% por trade).\n"
        f"- No es consejo financiero, analiza antes de entrar. 🍀"
    )
    return msg

# ============================================================
# Registro de señales + envío por correo
# ============================================================

def register_signal(d: dict):
    x = dict(d)
    x["ts"] = datetime.now(timezone.utc).isoformat()
    DAILY_SIGNALS.append(x)

def send_email_with_attachments(subject: str, body: str, attachments: list[tuple[str, bytes, str]]):
    if not (SMTP_EMAIL and SMTP_APP_PASSWORD and SMTP_TO):
        print("⚠️ SMTP no configurado; no se envía correo.")
        return
    msg = EmailMessage()
    msg["From"] = SMTP_EMAIL
    msg["To"] = SMTP_TO
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

# ============================================================
# ANALISIS PRINCIPAL DEL SÍMBOLO
# ============================================================

def bias_from_4h(df4: pd.DataFrame):
    if f"EMA_{EMA_SWING_4H}" not in df4.columns:
        return 0
    close = float(df4["close"].iloc[-1])
    ema_swing = float(df4[f"EMA_{EMA_SWING_4H}"].iloc[-1])
    if close > ema_swing:
        return 1
    if close < ema_swing:
        return -1
    return 0

def bias_from_emacross(df: pd.DataFrame, ema_fast: int, ema_mid: int, ema_long: int):
    if any(f"EMA_{x}" not in df.columns for x in (ema_fast, ema_mid, ema_long)):
        return 0
    last = df.iloc[-1]
    f = float(last[f"EMA_{ema_fast}"])
    m = float(last[f"EMA_{ema_mid}"])
    l = float(last[f"EMA_{ema_long}"])
    if f > m > l:
        return 1
    if f < m < l:
        return -1
    return 0

def analyze_symbol(symbol: str, mode_name: str):
    cfg = MODE_CFG[mode_name]

    # 4h / 1h / 30m / 15m / 5m
    df4   = fetch_ohlcv_first_ok(symbol, "4h",  limit=400)
    df1h  = fetch_ohlcv_first_ok(symbol, "1h",  limit=400)
    df30m = fetch_ohlcv_first_ok(symbol, "30m", limit=400)
    df15m = fetch_ohlcv_first_ok(symbol, "15m", limit=400)
    df5m  = fetch_ohlcv_first_ok(symbol, "5m",  limit=400)

    if df4.empty or df1h.empty or df30m.empty or df15m.empty or df5m.empty:
        return None

    df4   = compute_indicators(df4,   (EMA_SWING_4H,))
    df1h  = compute_indicators(df1h,  EMA_MACRO_1H)
    df30m = compute_indicators(df30m, EMA_CONF_30M)
    df15m = compute_indicators(df15m, EMA_TRIG_15M + (20,))
    df5m  = compute_indicators(df5m,  EMA_EXEC_5M + (20,))

    # 1) Bias Swing 4h
    b4 = bias_from_4h(df4)

    # 2) Bias Macro 1h (OBLIGATORIO)
    b1 = bias_from_emacross(df1h, *EMA_MACRO_1H)

    if b1 == 0:
        return None

    # 3) Sesgo Confirm 30m (suma puntos, no obligatorio)
    b30 = bias_from_emacross(df30m, *EMA_CONF_30M)

    # Determinar side
    if b1 > 0:
        side = "LONG"
    else:
        side = "SHORT"

    # 4) Pullback + Hook en 15m
    pb = pullback_ratio(df15m, side)
    if not (cfg["pb_min"] <= pb <= cfg["pb_max"]):
        return None

    if not hook_ema50_rsi(df15m, side, cfg["hook_eps"]):
        return None

    # 5) Confirmación 5m
    #   - EMAs alineadas en mismo sentido
    #   - RSI del lado correcto
    if side == "LONG":
        cond_ema_exec = bias_from_emacross(df5m, *EMA_EXEC_5M) >= 0
        cond_rsi_exec = float(df5m["RSI"].iloc[-1]) >= 45
    else:
        cond_ema_exec = bias_from_emacross(df5m, *EMA_EXEC_5M) <= 0
        cond_rsi_exec = float(df5m["RSI"].iloc[-1]) <= 55

    if not (cond_ema_exec and cond_rsi_exec):
        return None

    # 6) SL / TP / RR
    price = float(df5m["close"].iloc[-1])
    atr_val = float(df15m["ATR"].iloc[-1])
    sl, tp1, tp2, rr1 = rr_sl_tp(price, atr_val, df15m, side, mode_name)
    if rr1 < cfg["rr_min"]:
        return None

    # 7) Estructura + pivot fractal (1h)
    structure_label, structure_ok, pivot_near, pivot_label = detect_structure(df1h, side)

    # 8) Momentum 15m
    mom_dom, mom_label = momentum_bars(df15m, side, bars=9)

    # 9) EMA20 distance (15m)
    ema20_dist, ema20_label = ema20_distance(df15m)

    # 10) VWAP contexto (5m)
    vwap_label = vwap_context(df5m, side)

    # 11) Sesión + timestamp local aproximado
    ts_last = df5m.index[-1]
    session_name, ts_str = session_from_ts(ts_last)

    # ========================================================
    # SCORE V1 / V2
    # ========================================================

    score_v1 = 0
    score_v1_max = 9

    # V1: cosas "estructurales"
    if b4 != 0 and (b4 > 0 and side == "LONG" or b4 < 0 and side == "SHORT"):
        score_v1 += 1  # swing a favor
    if b1 != 0:
        score_v1 += 1  # macro definido
    if b30 != 0 and (b30 > 0 and side == "LONG" or b30 < 0 and side == "SHORT"):
        score_v1 += 1  # confirm 30m
    if cfg["pb_min"] <= pb <= cfg["pb_max"]:
        score_v1 += 1  # pullback en rango
    score_v1 += 1      # hook 15m (si llegamos aquí es que lo cumplió)
    if cond_ema_exec:
        score_v1 += 1  # EMA exec a favor
    if cond_rsi_exec:
        score_v1 += 1  # RSI exec a favor
    if structure_ok:
        score_v1 += 1  # estructura LL/LH o HH/HL a favor
    if mom_dom >= 5:
        score_v1 += 1  # momentum decente

    # V2: contexto (0–100 raw)
    score_v2_raw = 0

    if ema20_dist is not None and abs(ema20_dist) <= 0.01:
        score_v2_raw += 15
    if vwap_label:
        score_v2_raw += 15
    if pivot_near:
        score_v2_raw += 15
    if session_name in ("Londres", "USA"):
        score_v2_raw += 10
    if rr1 >= cfg["rr_min"] + 0.3:
        score_v2_raw += 15
    if structure_label:
        score_v2_raw += 10
    if mom_dom >= 6:
        score_v2_raw += 10
    if abs(pb - (cfg["pb_min"] + cfg["pb_max"])/2) <= 0.05:
        score_v2_raw += 10

    score_v2_raw = min(100, score_v2_raw)
    gates_passed = 0  # si luego quieres, puedes mapear 8 "gates" concretos

    # Resultado final
    return {
        "symbol": symbol,
        "side": side,
        "price": price,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rr1": rr1,
        "score_v1": score_v1,
        "score_v1_max": score_v1_max,
        "score_v2_raw": score_v2_raw,
        "gates": gates_passed,
        "ema20_label": ema20_label,
        "momentum_label": mom_label,
        "structure_label": structure_label,
        "structure_ok": structure_ok,
        "vwap_label": vwap_label,
        "pivot_label": pivot_label,
        "session_name": session_name,
        "ts_str": ts_str,
    }

# ============================================================
# Loops principales
# ============================================================

async def monitor_loop():
    await startup_notice()
    while True:
        if not MONITOR_ACTIVE:
            await asyncio.sleep(3)
            continue

        mode_name = MODE["current"]
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Escaneando en modo {mode_name.upper()}...")

        # Escaneo por bloques para no reventar nada
        batch_size = 40
        for i in range(0, len(ACTIVE_PAIRS), batch_size):
            batch = ACTIVE_PAIRS[i:i+batch_size]
            for sym in batch:
                try:
                    res = analyze_symbol(sym, mode_name)
                    if not res:
                        continue

                    side = res["side"]
                    if not can_send(sym, side):
                        continue

                    msg = build_signal_message(
                        symbol       = res["symbol"],
                        side         = res["side"],
                        tf_exec      = "5m",
                        price        = res["price"],
                        sl           = res["sl"],
                        tp1          = res["tp1"],
                        tp2          = res["tp2"],
                        score_v1     = res["score_v1"],
                        score_v1_max = res["score_v1_max"],
                        score_v2_raw = res["score_v2_raw"],
                        gates_passed = res["gates"],
                        ema20_label  = res["ema20_label"],
                        momentum_label = res["momentum_label"],
                        structure_label = res["structure_label"],
                        structure_ok    = res["structure_ok"],
                        vwap_label      = res["vwap_label"],
                        pivot_label     = res["pivot_label"],
                        session_name    = res["session_name"],
                        ts_str          = res["ts_str"],
                    )

                    await send_tg(msg)

                    register_signal({
                        "symbol": sym,
                        "side": side,
                        "price": res["price"],
                        "sl": res["sl"],
                        "tp1": res["tp1"],
                        "tp2": res["tp2"],
                        "rr1": res["rr1"],
                        "mode": mode_name,
                    })
                    mark_sent(sym, side)
                except Exception as e:
                    print(f"⚠️ Error analizando {sym}: {e}")

        await asyncio.sleep(RUN_EVERY_SEC)

async def scheduler_loop():
    last_excel_day = None
    while True:
        now = datetime.now(timezone.utc)
        # Enviar Excel diario cerca de medianoche UTC
        if last_excel_day != now.date() and now.hour == 23 and now.minute >= 55:
            try:
                await email_daily_signals_excel()
            except Exception as e:
                print("email_daily_signals_excel error:", e)
            last_excel_day = now.date()
        await asyncio.sleep(30)

# ============================================================
# Loop simple de comandos Telegram (getUpdates)
# ============================================================

async def telegram_commands_loop():
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("⚠️ Telegram no configurado; comandos deshabilitados.")
        return
    print("✅ Loop de comandos Telegram iniciado (getUpdates).")
    while True:
        try:
            params = {
                "timeout": 20,
                "offset": STATE["tg_update_offset"] + 1,
            }
            resp = requests.get(TG_API_BASE + "/getUpdates", params=params, timeout=25)
            if resp.status_code != 200:
                await asyncio.sleep(5)
                continue
            data = resp.json()
            if not data.get("ok"):
                await asyncio.sleep(5)
                continue
            updates = data.get("result", [])
            for upd in updates:
                STATE["tg_update_offset"] = max(STATE["tg_update_offset"], upd["update_id"])
                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if TELEGRAM_CHAT_ID and chat_id != TELEGRAM_CHAT_ID:
                    continue
                text = (msg.get("text") or "").strip()
                if not text.startswith("/"):
                    continue
                await handle_command(text)
        except Exception as e:
            print("⚠️ telegram_commands_loop error:", e)
        await asyncio.sleep(2)

async def handle_command(text: str):
    global MONITOR_ACTIVE
    parts = text.lower().split()
    cmd = parts[0]

    if cmd == "/start":
        MONITOR_ACTIVE = True
        await send_tg("✅ Bot ACTIVADO (escaneo reanudado).")
    elif cmd == "/stop":
        MONITOR_ACTIVE = False
        await send_tg("🛑 Bot DETENIDO (no se enviarán nuevas señales).")
    elif cmd == "/status":
        await send_tg(
            f"📊 <b>STATUS</b>\n"
            f"• Proyecto: <b>{PROJECT_NAME}</b>\n"
            f"• Pares activos: <b>{len(ACTIVE_PAIRS)}</b>\n"
            f"• Modo: <b>{MODE['current'].upper()}</b>\n"
            f"• Monitoreo: <b>{'ACTIVO' if MONITOR_ACTIVE else 'DETENIDO'}</b>\n"
            f"• Intervalo escaneo: {RUN_EVERY_SEC//60} min"
        )
    elif cmd == "/mode" and len(parts) >= 2:
        new_mode = parts[1]
        if new_mode in MODE_CFG:
            MODE["current"] = new_mode
            await send_tg(f"⚙️ Modo cambiado a <b>{new_mode.upper()}</b>.")
        else:
            await send_tg("Usa: /mode suave | /mode normal | /mode sniper")
    elif cmd == "/help":
        await send_tg(
            "📘 Comandos:\n"
            "/start – Activa escaneo\n"
            "/stop – Pausa escaneo\n"
            "/status – Ver estado actual\n"
            "/mode suave|normal|sniper – Cambiar modo\n"
            "/help – Este mensaje"
        )
    else:
        await send_tg("Comando no reconocido. Usa /help.")

# ============================================================
# FastAPI keep-alive para Render
# ============================================================

app = FastAPI()

@app.get("/ping")
async def ping():
    return {
        "ok": True,
        "service": PROJECT_NAME,
        "time": datetime.utcnow().isoformat() + "Z",
        "mode": MODE["current"],
        "pairs": len(ACTIVE_PAIRS),
    }

def start_http():
    def _run():
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), log_level="warning")
    th = threading.Thread(target=_run, daemon=True)
    th.start()

# ============================================================
# Entrypoint
# ============================================================

async def main_async():
    start_http()
    await asyncio.gather(
        monitor_loop(),
        scheduler_loop(),
        telegram_commands_loop(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except RuntimeError:
        loop = asyncio.get_event_loop()
        loop.create_task(main_async())
        loop.run_forever()
