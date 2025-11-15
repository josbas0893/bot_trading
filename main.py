# ==============================
# Bot Swing/Macro v3 – Render / Replit
# - 3 modos: suave, normal, sniper
# - Filtro de shitcoins por liquidez
# - Entrenamiento de pares + escaneo por bloques
# ==============================

import os, asyncio, time, math, threading, ssl, io
from datetime import datetime, timezone
from email.message import EmailMessage

import ccxt           # type: ignore
import numpy as np    # type: ignore
import pandas as pd   # type: ignore

from fastapi import FastAPI
import uvicorn
from dotenv import load_dotenv
from telegram import Bot  # type: ignore

# =============================
# ENV / CONFIG
# =============================
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

SMTP_EMAIL        = os.getenv("SMTP_EMAIL", "")
SMTP_APP_PASSWORD = os.getenv("SMTP_APP_PASSWORD", "")
SMTP_TO           = os.getenv("SMTP_TO", SMTP_EMAIL or "")

PROJECT_NAME = os.getenv("PROJECT_NAME", "SwingMacroBotV3")

# Exchanges – dejamos solo los que no dan error por región
EX_NAMES       = [s.strip() for s in os.getenv("EXCHANGES", "okx,kucoin").split(",") if s.strip()]
PAIRS_ENV      = os.getenv("PAIRS", "").strip()
MAX_PAIRS_ENV  = int(os.getenv("MAX_PAIRS", "200"))

# Modo del bot
BOT_MODE       = os.getenv("BOT_MODE", "normal").lower()  # suave / normal / sniper
RUN_EVERY_SEC  = int(os.getenv("RUN_EVERY_SEC", "300"))   # 300 = 5 min
CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", "40"))       # símbolos por pasada

# Filtro de liquidez / shitcoins
SHIT_FILTER    = os.getenv("SHIT_FILTER", "false").lower() in ("1", "true", "yes", "y")
MIN_LIQ_USD    = float(os.getenv("MIN_LIQ_USD", "80000"))  # mínimo volumen en 4h (precio * volumen)

# Señales a Excel diario
ENABLE_DAILY_XLS = os.getenv("ENABLE_DAILY_XLS", "true").lower() in ("1","true","yes","y")

# =============================
# MODO: UMBRALES POR MODO
# =============================
MODE_CONF = {
    "suave": {
        "min_score": 30,
        "min_rr": 1.2,
        "pullback_min": 0.25,
        "pullback_max": 0.85,
    },
    "normal": {
        "min_score": 45,
        "min_rr": 1.5,
        "pullback_min": 0.3,
        "pullback_max": 0.8,
    },
    "sniper": {
        "min_score": 60,
        "min_rr": 1.8,
        "pullback_min": 0.35,
        "pullback_max": 0.75,
    },
}
if BOT_MODE not in MODE_CONF:
    BOT_MODE = "normal"

CONF = MODE_CONF[BOT_MODE]

# =============================
# Estado global
# =============================
EX_OBJS = {}
ALL_PAIRS: list[str] = []
TRAINED_GOOD: list[str] = []
TRAINED_BAD: list[str]  = []
BATCHES: list[list[str]] = []
STATE = {
    "started": False,
    "batch_idx": 0,
    "last_signal": {},  # (symbol, side) -> timestamp
}
DAILY_SIGNALS: list[dict] = []

# =============================
# Telegram helper (solo salida)
# =============================
bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None

async def send_tg(text: str):
    """Envía mensaje a Telegram o imprime en logs si no hay config."""
    if not bot or not TELEGRAM_CHAT_ID:
        print("TG>", text.replace("\n", " | "))
        return
    try:
        await asyncio.to_thread(
            bot.send_message,
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="HTML",
        )
    except Exception as e:
        print("❌ Telegram error:", e)


# =============================
# Email helper (Excel diario)
# =============================
def send_email_with_attachments(subject: str, body: str, attachments: list[tuple[str, bytes, str]]):
    if not (SMTP_EMAIL and SMTP_APP_PASSWORD and SMTP_TO):
        print("⚠️ SMTP no configurado, no se envía email.")
        return
    msg = EmailMessage()
    msg["From"] = SMTP_EMAIL
    msg["To"]   = SMTP_TO
    msg["Subject"] = subject
    msg.set_content(body)

    for filename, content, mime in attachments:
        maintype, subtype = mime.split("/", 1)
        msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

    ctx = ssl.create_default_context()
    import smtplib
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(SMTP_EMAIL, SMTP_APP_PASSWORD)
        s.send_message(msg)


async def email_daily_signals_excel():
    if not ENABLE_DAILY_XLS:
        return
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
        [(f"signals_{day}.xlsx", buf.read(),
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")],
    )
    DAILY_SIGNALS.clear()


def register_signal(row: dict):
    r = dict(row)
    r["ts"] = datetime.now(timezone.utc).isoformat()
    DAILY_SIGNALS.append(r)


# =============================
# Exchange / Pairs
# =============================
def init_exchanges():
    global EX_OBJS
    for name in EX_NAMES:
        try:
            klass = getattr(ccxt, name)
            opts = {"enableRateLimit": True}
            if name in ("kucoin",):
                opts["options"] = {"defaultType": "future"}
            ex = klass(opts)
            ex.load_markets()
            EX_OBJS[name] = ex
            print(f"✅ Conectado a {name} con {len(ex.symbols)} símbolos.")
        except Exception as e:
            print(f"⚠️ No se pudo iniciar {name}: {e}")


def _basic_usdt_filter(sym: str) -> bool:
    if not sym.endswith("/USDT"):
        return False
    if "UP/" in sym or "DOWN/" in sym:
        return False
    return True


def build_pairs_auto(limit: int) -> list[str]:
    agg = []
    seen = set()
    for ex in EX_OBJS.values():
        try:
            syms = [s for s in ex.symbols if _basic_usdt_filter(s)]
        except Exception:
            continue
        for s in syms:
            if s not in seen:
                agg.append(s)
                seen.add(s)
            if len(agg) >= limit:
                break
        if len(agg) >= limit:
            break
    return sorted(agg)


def init_pairs_and_batches():
    global ALL_PAIRS, BATCHES
    if PAIRS_ENV:
        ALL_PAIRS = [s.strip() for s in PAIRS_ENV.split(",") if s.strip()]
    else:
        ALL_PAIRS = build_pairs_auto(limit=MAX_PAIRS_ENV)

    # dedup & recorte
    seen = set()
    cleaned: list[str] = []
    for s in ALL_PAIRS:
        if s not in seen:
            cleaned.append(s)
            seen.add(s)
    ALL_PAIRS = cleaned[:MAX_PAIRS_ENV]

    # crear batches
    BATCHES = [
        ALL_PAIRS[i:i + CHUNK_SIZE]
        for i in range(0, len(ALL_PAIRS), CHUNK_SIZE)
    ]
    if not BATCHES:
        BATCHES = [[]]

    print(f"📦 Pares totales: {len(ALL_PAIRS)}, batches: {len(BATCHES)}")


# =============================
# Indicadores simples
# =============================
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
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    df["ATR_14"] = tr.rolling(14).mean()
    return df.dropna()


# =============================
# Fetch OHLCV
# =============================
def fetch_ohlcv_first_ok(symbol: str, timeframe: str, limit: int = 300) -> pd.DataFrame:
    for ex in EX_OBJS.values():
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not data:
                continue
            df = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms")
            df.set_index("ts", inplace=True)
            # quitamos la vela en curso
            df = df.iloc[:-1]
            return df
        except Exception:
            continue
    return pd.DataFrame()


# =============================
# Bias y Pullback
# =============================
def swing_bias_4h(df4: pd.DataFrame) -> int:
    """EMA 50 / 100 en 4h para bias principal."""
    if df4.empty or len(df4) < 50:
        return 0
    last = df4.iloc[-1]
    c = last["close"]
    ema50 = last["EMA_50"]
    ema100 = last["EMA_100"]
    if c > ema50 and ema50 >= ema100:
        return 1
    if c < ema50 and ema50 <= ema100:
        return -1
    return 0


def macro_bias_1h(df1: pd.DataFrame) -> int:
    """Cruce EMA 12/50/200 en 1h (obligatorio)."""
    if df1.empty or len(df1) < 60:
        return 0
    last = df1.iloc[-1]
    c = last["close"]
    e12 = last["EMA_12"]
    e50 = last["EMA_50"]
    e200 = last["EMA_200"]

    if c > e50 > e200 and e12 > e50:
        return 1
    if c < e50 < e200 and e12 < e50:
        return -1
    return 0


def confirm_bias_30m(df30: pd.DataFrame) -> int:
    """Mismo cruce 12/50/200 en 30m para puntos extra."""
    if df30.empty or len(df30) < 60:
        return 0
    last = df30.iloc[-1]
    c = last["close"]
    e12 = last["EMA_12"]
    e50 = last["EMA_50"]
    e200 = last["EMA_200"]

    if c > e50 > e200 and e12 > e50:
        return 1
    if c < e50 < e200 and e12 < e50:
        return -1
    return 0


def pullback_ratio_15m(df15: pd.DataFrame, is_long: bool) -> float | None:
    """Relación de pullback dentro de un swing en 15m (0 = low, 1 = high)."""
    if df15.empty or len(df15) < 80:
        return None
    seg = df15.tail(80)
    swing_low = seg["low"].min()
    swing_high = seg["high"].max()
    last_price = seg["close"].iloc[-1]
    if swing_high <= swing_low:
        return None
    if is_long:
        return (last_price - swing_low) / (swing_high - swing_low)
    else:
        # para short, medimos desde arriba
        return (swing_high - last_price) / (swing_high - swing_low)


def trigger_hook_15m(df15: pd.DataFrame, is_long: bool, cfg: dict) -> bool:
    """Hook en 15m: rebote EMA50 + RSI."""
    if df15.empty or len(df15) < 5:
        return False
    last   = df15.iloc[-1]
    prev   = df15.iloc[-2]
    c      = last["close"]
    ema50  = last["EMA_50"]
    rsi    = last["RSI"]
    rsi_p  = prev["RSI"]

    dist = (c - ema50) / ema50  # relativo

    # rangos de pullback por modo
    dist_ok = abs(dist) <= 0.02   # ±2% alrededor de EMA50

    if is_long:
        # vela alcista y RSI girando hacia arriba desde zona baja
        return dist_ok and (c > last["open"]) and (rsi > rsi_p) and (rsi_p < 55)
    else:
        return dist_ok and (c < last["open"]) and (rsi < rsi_p) and (rsi_p > 45)


def execution_ok_5m(df5: pd.DataFrame, is_long: bool) -> bool:
    """Refinamiento en 5m: dirección a favor."""
    if df5.empty or len(df5) < 20:
        return False
    last = df5.iloc[-1]
    c    = last["close"]
    e20  = last["EMA_20"]
    e50  = last["EMA_50"]
    rsi  = last["RSI"]
    if is_long:
        return (c > e20 > e50) and (rsi > 50)
    else:
        return (c < e20 < e50) and (rsi < 50)


def liquidity_4h(df4: pd.DataFrame) -> float:
    """Liquidez aproximada en USD usando último precio y volumen medio 4h."""
    if df4.empty or len(df4) < 10:
        return 0.0
    last_price = float(df4["close"].iloc[-1])
    avg_vol    = float(df4["volume"].tail(40).mean())
    return last_price * avg_vol


def rr_from_sl_tp(price: float, sl: float, tp: float) -> float:
    risk   = abs(price - sl)
    reward = abs(tp - price)
    if risk <= 0:
        return 0.0
    return reward / risk


def make_sl_tp(price: float, is_long: bool, df15: pd.DataFrame, atr: float) -> tuple[float, float, float]:
    """SL/TP básicos usando ATR + rango reciente 1h/15m."""
    if df15.empty:
        # fallback simple
        if is_long:
            sl = price - 1.5 * atr
            tp1 = price + 1.5 * atr
            tp2 = price + 3.0 * atr
        else:
            sl = price + 1.5 * atr
            tp1 = price - 1.5 * atr
            tp2 = price - 3.0 * atr
        return float(sl), float(tp1), float(tp2)

    seg = df15.tail(60)
    hi  = float(seg["high"].max())
    lo  = float(seg["low"].min())
    if is_long:
        sl = min(lo, price - 1.5 * atr)
        tp1 = price + max(1.5 * atr, (hi - price) * 0.6)
        tp2 = price + max(3.0 * atr, (hi - price) * 1.0)
    else:
        sl = max(hi, price + 1.5 * atr)
        tp1 = price - max(1.5 * atr, (price - lo) * 0.6)
        tp2 = price - max(3.0 * atr, (price - lo) * 1.0)
    return float(sl), float(tp1), float(tp2)


def fmt_price(x: float) -> str:
    if x < 0:
        x = abs(x)
    if x == 0:
        return "0"
    if x < 1:
        return f"{x:.6f}"
    if x < 100:
        return f"{x:.4f}"
    return f"{x:.2f}"


def can_send(symbol: str, side: str, cooldown_min: int = 30) -> bool:
    key = (symbol, side)
    last = STATE["last_signal"].get(key, 0)
    return (time.time() - last) > cooldown_min * 60


def mark_sent(symbol: str, side: str):
    STATE["last_signal"][(symbol, side)] = time.time()


def build_signal_message(
    symbol: str,
    side: str,
    tf_str: str,
    price: float,
    sl: float,
    tp1: float,
    tp2: float,
    score: float,
    swing_bias: int,
    macro_bias: int,
    confirm_bias: int,
    pullback_r: float | None,
) -> str:
    side_emoji = "🟢 LONG 📈" if side == "LONG" else "🔴 SHORT 📉"
    mode_label = BOT_MODE.upper()
    bias_txt = {
        1: "Alcista",
        -1: "Bajista",
        0: "Neutral"
    }

    pull_txt = "N/A"
    if pullback_r is not None:
        pull_txt = f"{pullback_r*100:0.1f}%"

    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    msg = (
        f"✨ SEÑAL {mode_label} ✨\n\n"
        f"💱 <b>{symbol}</b>\n"
        f"⏰ TF entrada: <b>{tf_str}</b>\n"
        f"🕒 Hora: <b>{now_str}</b>\n\n"
        f"➡️ Dirección: <b>{side_emoji}</b>\n\n"
        f"💰 Entrada: <code>{fmt_price(price)}</code>\n"
        f"🛑 SL: <code>{fmt_price(sl)}</code>\n"
        f"🎯 TP1: <code>{fmt_price(tp1)}</code>\n"
        f"🎯 TP2: <code>{fmt_price(tp2)}</code>\n\n"
        f"📊 Bias 4h (Swing): <b>{bias_txt.get(swing_bias, 'N/A')}</b>\n"
        f"📊 Bias 1h (Macro): <b>{bias_txt.get(macro_bias, 'N/A')}</b>\n"
        f"📊 Bias 30m (Conf): <b>{bias_txt.get(confirm_bias, 'N/A')}</b>\n"
        f"📉 Pullback 15m: <b>{pull_txt}</b>\n"
        f"⭐ Score interno: <b>{score:.1f}</b>\n\n"
        f"⚠️ Gestión sugerida:\n"
        f" • TP1: 30% + mover SL a BE\n"
        f" • TP2: 70% o dejar correr según tu plan\n\n"
        f"Esto NO es consejo financiero. Opera con gestión de riesgo. 🍀"
    )
    return msg


# =============================
# ENTRENAMIENTO DE PARES
# =============================
async def train_pairs():
    """Fase de entrenamiento simple para clasificar pares en buenos / malos."""
    global TRAINED_GOOD, TRAINED_BAD

    await send_tg("📥 Iniciando descarga de velas para <b>entrenamiento</b>…")
    good: list[str] = []
    bad: list[str]  = []

    # usaremos solo 4h + 1h para "entrenar"
    for sym in ALL_PAIRS:
        try:
            df4 = fetch_ohlcv_first_ok(sym, "4h", limit=120)
            df4 = compute_indicators(df4) if not df4.empty else df4
            df1 = fetch_ohlcv_first_ok(sym, "1h", limit=200)
            df1 = compute_indicators(df1) if not df1.empty else df1

            if df4.empty or df1.empty:
                bad.append(sym)
                continue

            liq = liquidity_4h(df4)
            if SHIT_FILTER and liq < MIN_LIQ_USD:
                bad.append(sym)
                continue

            sb = swing_bias_4h(df4)
            mb = macro_bias_1h(df1)
            # requerimos que al menos el macro no sea neutro
            if mb == 0:
                bad.append(sym)
                continue

            # "score" simple
            score = 0
            if sb == mb:
                score += 20
            elif sb == 0:
                score += 5
            else:
                score -= 10

            # bonus por volatilidad razonable
            atr_rel = float(df4["ATR_14"].iloc[-1] / df4["close"].iloc[-1])
            if 0.005 <= atr_rel <= 0.08:  # 0.5% a 8%
                score += 15
            else:
                score -= 5

            # bonus por tendencia relativamente limpia en 1h
            df1_seg = df1.tail(80)
            cmin, cmax = df1_seg["close"].min(), df1_seg["close"].max()
            if (cmax - cmin) / cmin > 0.03:
                score += 10

            if score >= 20:
                good.append(sym)
            else:
                bad.append(sym)

        except Exception as e:
            print(f"⚠️ Error entrenando {sym}: {e}")
            bad.append(sym)

    TRAINED_GOOD = good
    TRAINED_BAD  = bad

    # Mensajes 3, 4 y 5
    await send_tg("🧠 Entrenamiento básico de pares <b>completado</b>.")
    if good:
        chunks = [good[i:i+40] for i in range(0, len(good), 40)]
        for i, ch in enumerate(chunks, start=1):
            await send_tg(
                f"✅ Pares <b>aprobados</b> para la estrategia ({len(good)} total, parte {i}):\n"
                + ", ".join(ch)
            )
    if bad:
        chunks = [bad[i:i+40] for i in range(0, len(bad), 40)]
        for i, ch in enumerate(chunks, start=1):
            await send_tg(
                f"⚠️ Pares <b>rechazados</b> o de baja liquidez ({len(bad)} total, parte {i}):\n"
                + ", ".join(ch)
            )


def trained_universe() -> list[str]:
    """Devuelve la lista de pares que el bot debe usar tras entrenamiento."""
    if TRAINED_GOOD:
        return TRAINED_GOOD
    return ALL_PAIRS


# =============================
# ANALISIS DE UN SÍMBOLO
# =============================
def analyze_symbol(symbol: str) -> dict | None:
    """
    Aplica la lógica Swing (4h) + Macro (1h) + Conf (30m) + Gatillo (15m) + Exec (5m)
    y devuelve una señal si pasa el score mínimo del modo actual.
    """
    # 4h, 1h, 30m, 15m, 5m
    df4  = fetch_ohlcv_first_ok(symbol, "4h",  limit=200)
    df1  = fetch_ohlcv_first_ok(symbol, "1h",  limit=260)
    df30 = fetch_ohlcv_first_ok(symbol, "30m", limit=260)
    df15 = fetch_ohlcv_first_ok(symbol, "15m", limit=260)
    df5  = fetch_ohlcv_first_ok(symbol, "5m",  limit=260)

    if any(df.empty for df in (df4, df1, df30, df15, df5)):
        return None

    df4  = compute_indicators(df4)
    df1  = compute_indicators(df1)
    df30 = compute_indicators(df30)
    df15 = compute_indicators(df15)
    df5  = compute_indicators(df5)

    if any(len(df) < 60 for df in (df4, df1, df30, df15, df5)):
        return None

    # Shit filter adicional en runtime
    liq = liquidity_4h(df4)
    if SHIT_FILTER and liq < MIN_LIQ_USD:
        return None

    swing_b = swing_bias_4h(df4)
    macro_b = macro_bias_1h(df1)
    confirm_b = confirm_bias_30m(df30)

    if macro_b == 0:
        return None

    # Definimos lado principal por macro
    is_long = macro_b == 1
    side = "LONG" if is_long else "SHORT"

    # Pullback en 15m
    pb = pullback_ratio_15m(df15, is_long=is_long)
    if pb is None:
        return None

    if not (CONF["pullback_min"] <= pb <= CONF["pullback_max"]):
        # fuera de la zona de pullback saludable
        return None

    # Hook en 15m
    hook_ok = trigger_hook_15m(df15, is_long, CONF)
    if not hook_ok:
        return None

    # Exec en 5m
    exec_ok = execution_ok_5m(df5, is_long)
    if not exec_ok:
        # en modo suave podríamos permitirlo, pero por ahora exigimos
        if BOT_MODE != "suave":
            return None

    # Score
    score = 0.0

    # macro es base
    score += 25

    # swing alineado
    if swing_b == macro_b:
        score += 20
    elif swing_b == 0:
        score += 5
    else:
        score -= 15

    # confirm 30m
    if confirm_b == macro_b:
        score += 15
    elif confirm_b == 0:
        score += 0
    else:
        score -= 10

    # pullback en zona dorada: cuanto más cercano a 0.5–0.7 mejor
    center = 0.6
    dist_pb = abs(pb - center)
    if dist_pb < 0.1:
        score += 20
    elif dist_pb < 0.2:
        score += 10
    else:
        score += 3

    # hook + exec
    if hook_ok:
        score += 15
    if exec_ok:
        score += 10

    # chequeo de mínimo por modo
    if score < CONF["min_score"]:
        return None

    # SL / TP y RR
    price = float(df5["close"].iloc[-1])
    atr15 = float(df15["ATR_14"].iloc[-1])
    sl, tp1, tp2 = make_sl_tp(price, is_long, df15, atr15)
    rr1 = rr_from_sl_tp(price, sl, tp1)

    if rr1 < CONF["min_rr"]:
        return None

    return {
        "symbol": symbol,
        "side": side,
        "price": price,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "score": score,
        "swing_bias": swing_b,
        "macro_bias": macro_b,
        "confirm_bias": confirm_b,
        "pullback": pb,
    }


# =============================
# LOOPS PRINCIPALES
# =============================
async def monitor_loop():
    """Loop de trading – escanea por bloques usando el universo entrenado."""
    await send_tg(
        f"✅ <b>{PROJECT_NAME}</b> iniciado.\n"
        f"🎛️ Modo: <b>{BOT_MODE.upper()}</b>\n"
        f"📦 Pares totales: <b>{len(ALL_PAIRS)}</b>\n"
        f"⏱️ Escaneo cada <b>{RUN_EVERY_SEC//60} min</b> por bloques de <b>{CHUNK_SIZE}</b> símbolos."
    )

    # Fase de entrenamiento (mensajes 2,3,4,5)
    await train_pairs()

    while True:
        try:
            uni = trained_universe()
            if not uni:
                await asyncio.sleep(RUN_EVERY_SEC)
                continue

            # batch actual
            if not BATCHES:
                batch = uni
            else:
                idx = STATE["batch_idx"] % len(BATCHES)
                # intersección entre batch y universo entrenado
                batch = [s for s in BATCHES[idx] if s in uni]
                if not batch:
                    batch = uni

                STATE["batch_idx"] = (STATE["batch_idx"] + 1) % len(BATCHES)

            await send_tg(
                f"📡 Escaneando bloque {STATE['batch_idx']+1}/{len(BATCHES)} "
                f"({len(batch)} pares)…"
            )

            for sym in batch:
                try:
                    res = analyze_symbol(sym)
                    if not res:
                        continue

                    side = res["side"]
                    if not can_send(sym, side):
                        continue

                    msg = build_signal_message(
                        symbol=res["symbol"],
                        side=res["side"],
                        tf_str="5m (gatillo 15m / macro 1h / swing 4h)",
                        price=res["price"],
                        sl=res["sl"],
                        tp1=res["tp1"],
                        tp2=res["tp2"],
                        score=res["score"],
                        swing_bias=res["swing_bias"],
                        macro_bias=res["macro_bias"],
                        confirm_bias=res["confirm_bias"],
                        pullback_r=res["pullback"],
                    )
                    await send_tg(msg)
                    mark_sent(sym, side)
                    register_signal(res)
                except Exception as e:
                    print(f"⚠️ Error analizando {sym}: {e}")

        except Exception as e:
            print("⚠️ Error en monitor_loop:", e)

        await asyncio.sleep(RUN_EVERY_SEC)


async def scheduler_loop():
    """Loop para tareas temporales: envío de Excel al final del día."""
    last_day = None
    while True:
        now = datetime.now(timezone.utc)
        day = now.date()
        if last_day is None:
            last_day = day

        # envío al cambio de día
        if day != last_day:
            try:
                await email_daily_signals_excel()
            except Exception as e:
                print("email_daily_signals_excel error:", e)
            last_day = day

        await asyncio.sleep(30)


# =============================
# FastAPI / Keep-alive
# =============================
app = FastAPI()

@app.get("/ping")
async def ping():
    return {
        "ok": True,
        "service": PROJECT_NAME,
        "time": datetime.now(timezone.utc).isoformat(),
        "mode": BOT_MODE,
        "pairs_total": len(ALL_PAIRS),
        "trained_good": len(TRAINED_GOOD),
    }


def start_http():
    def _run():
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=int(os.environ.get("PORT", "8080")),
            log_level="warning",
        )
    th = threading.Thread(target=_run, daemon=True)
    th.start()


# =============================
# Entrypoint
# =============================
async def main_async():
    init_exchanges()
    init_pairs_and_batches()
    start_http()
    await asyncio.gather(
        monitor_loop(),
        scheduler_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except RuntimeError:
        # Para entornos que ya tienen loop (Replit, etc.)
        loop = asyncio.get_event_loop()
        loop.create_task(main_async())
        loop.run_forever()
