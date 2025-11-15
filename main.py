# ======================================================================
# Bot V4 – Render 24/7  (polars, modos dinámicos, filtros IA-reglas)
# SIN ApplicationBuilder / Updater (solo envío a Telegram por HTTP)
# ======================================================================

import os, time, math, asyncio, threading
from datetime import datetime, UTC
from email.message import EmailMessage
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
MAX_PAIRS           = int(os.getenv("MAX_PAIRS", "150"))
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
PROJECT_NAME        = os.getenv("PROJECT_NAME", "BotV4")
RUN_EVERY_SEC       = int(os.getenv("RUN_EVERY_SEC", "300"))
DATA_DIR            = "data"
BACKTEST_DAYS       = 60     # referencia, no filtro duro

# ---------- MODOS DINÁMICOS ----------
MODOS = {
    "suave":  {"wr": 0.50, "trades": 3,  "desc": "Suave – más señales"},
    "normal": {"wr": 0.65, "trades": 10, "desc": "Normal – balance"},
    "sniper": {"wr": 0.80, "trades": 20, "desc": "Sniper – muy selectivo"}
}
MODE = {"current": "normal"}   # modo por defecto

# score mínimo por modo (1–5 estrellas)
SCORE_MIN = {
    "suave":  2,
    "normal": 3,
    "sniper": 4,
}

MONITOR_ACTIVE = True
STATE = {
    "last_sent": {},     # (symbol, side) -> timestamp
    "daily_count": defaultdict(int),  # (symbol, side, date) -> count
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
        print(f"[TG SIMULADO] {text}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    def _post():
        try:
            r = requests.post(url, json=payload, timeout=10)
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
            df = pl.DataFrame(data, schema=["ts", "open", "high", "low", "close", "volume"])
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
# CONSTRUCCIÓN DE PARES (solo USDT que existan en Bitunix)
# ====================================================================
def build_pairs_list():
    bitunix = EX_OBJS.get("bitunix")
    if not bitunix:
        bitunix = ccxt.bitunix({"enableRateLimit": True})
        bitunix.load_markets()
    bitunix_syms = {s for s in bitunix.symbols if s.endswith("/USDT")}

    agg, seen = [], set()
    for ex in EX_OBJS.values():
        try:
            syms = [s for s in ex.symbols if s.endswith("/USDT") and s in bitunix_syms]
            for s in syms:
                if s not in seen:
                    agg.append(s)
                    seen.add(s)
                if len(agg) >= MAX_PAIRS:
                    break
            if len(agg) >= MAX_PAIRS:
                break
        except Exception:
            continue
    return sorted(agg)[:MAX_PAIRS]

# ====================================================================
# HELPERS DE VOLATILIDAD
# ====================================================================
def has_min_daily_range(df5: pl.DataFrame, min_pct: float = 0.015) -> bool:
    """
    Comprueba que el rango medio diario (high-low) sea al menos min_pct del precio medio.
    Evita pares que se mueven muy poquito (lateral aburrido).
    """
    if len(df5) < 100:
        return False
    window = 288
    if len(df5) < window:
        df_last = df5
    else:
        df_last = df5.tail(window)
    mid_price = df_last["close"].mean()
    avg_range = (df_last["high"] - df_last["low"]).mean()
    if mid_price is None or avg_range is None:
        return False
    try:
        return float(avg_range) / float(mid_price) >= min_pct
    except ZeroDivisionError:
        return False

# ====================================================================
# BACK-TEST INDIVIDUAL (con polars)
# ====================================================================
def backtest_pare(symbol):
    df5 = load_or_download(symbol, "5m")
    if len(df5) < 500:
        return {"trades": 0, "wr": 0, "df": pl.DataFrame()}
    if not has_min_daily_range(df5, min_pct=0.015):
        return {"trades": 0, "wr": 0, "df": pl.DataFrame()}

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
    for i in range(60, len(df5)-20):
        c     = df5["close"][i]
        ema12 = df5["EMA_12"][i]
        ema50 = df5["EMA_50"][i]
        ema200= df5["EMA_200"][i]
        rsi   = df5["RSI"][i]
        if pl.is_nan(c):
            continue

        is_long = None
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
    return {"trades": len(df_trades), "wr": float(wr or 0), "df": df_trades}

# ====================================================================
# MENSAJE TELEGRAM: APROBADOS vs NO APROBADOS
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
# INDICADORES (en vivo) con polars
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
# ANÁLISIS EN VIVO (solo pares aprobados)
# ====================================================================
APROBADOS = []

def analiza_vivo(symbol: str):
    df5 = load_or_download(symbol, "5m")
    if len(df5) < 60:
        return None

    if not has_min_daily_range(df5, min_pct=0.015):
        return None

    df5 = compute_indicators(df5)
    if len(df5) < 20:
        return None

    last = df5.tail(1)
    c     = last["close"][0]
    ema12 = last["EMA_12"][0]
    ema50 = last["EMA_50"][0]
    ema200= last["EMA_200"][0]
    rsi   = last["RSI"][0]
    vol   = last["volume"][0]

    if pl.is_nan(c) or pl.is_nan(ema12) or pl.is_nan(ema50) or pl.is_nan(ema200) or pl.is_nan(rsi):
        return None

    if len(df5) >= 21:
        vwin = df5.tail(21)
        v_mean = vwin["volume"][:-1].mean()
        if v_mean is not None and v_mean > 0 and vol < 1.5 * float(v_mean):
            return None

    score = 0

    is_long = None
    if c > ema200*0.998 and ema12 > ema50*0.998 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
        is_long = True
        score += 2
    elif c < ema200*1.002 and ema12 < ema50*1.002 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
        is_long = False
        score += 2

    if is_long is None:
        return None

    score += 1  # rango diario ya filtrado

    if 40 <= rsi <= 60:
        score += 1

    mode = MODE["current"]
    if score < SCORE_MIN.get(mode, 3):
        return None

    atr_like = (last["high"][0] - last["low"][0])
    if pl.is_nan(atr_like) or atr_like <= 0:
        return None

    sl  = c - atr_like*1.8 if is_long else c + atr_like*1.8
    tp1 = c + atr_like*1.8*1.5 if is_long else c - atr_like*1.8*1.5
    tp2 = c + atr_like*1.8*2.5 if is_long else c - atr_like*1.8*2.5

    rr  = abs(tp1 - c) / max(abs(c - sl), 1e-9)
    if rr < 1.6:
        return None

    side = "LONG" if is_long else "SHORT"

    return {
        "side": side,
        "price": float(c),
        "sl": float(sl),
        "tp1": float(tp1),
        "tp2": float(tp2),
        "rr": float(rr),
        "score": int(min(score, 5))
    }

# ====================================================================
# ALERTA / REGISTRO
# ====================================================================
def fmt_price(x):
    try:
        return f"{x:.6f}" if x < 1 else (f"{x:.4f}" if x < 100 else f"{x:.2f}")
    except:
        return str(x)

def build_alert(symbol, side, price, sl, tp1, tp2, rr, score=None):
    dir_emo = "🟢 LONG 📈" if side == "LONG" else "🔴 SHORT 📉"
    stars = ""
    if score is not None and score > 0:
        stars = "\n⭐ Calidad señal: " + "★"*score + f"  (score {score}/5)"

    return (
        f"✨ <b>SEÑAL DE TRADING</b> ✨\n\n"
        f"💰 <b>ACTIVO:</b> {symbol}\n"
        f"📉 <b>TEMPORALIDAD:</b> 15m / 5m\n"
        f"📍 <b>SETUP:</b> EMA + RSI + rango y volumen\n"
        f"➡️ <b>DIRECCIÓN:</b> {dir_emo}\n"
        f"📊 <b>ENTRADA (Entry):</b> <code>{fmt_price(price)}</code>\n"
        f"🛑 <b>STOP LOSS:</b> <code>{fmt_price(sl)}</code>\n"
        f"🎯 <b>TP1:</b> <code>{fmt_price(tp1)}</code>\n"
        f"🎯 <b>TP2:</b> <code>{fmt_price(tp2)}</code>\n"
        f"🎯 <b>TP3:</b> <code>{fmt_price(tp2 + (tp2 - tp1))}</code>\n\n"
        f"📈 <b>RR:</b> {rr:.2f}{stars}\n\n"
        f"⚠️ <b>Gestión:</b> mover SL a BE al alcanzar TP1. "
        f"Opera bajo tu propio riesgo. 🍀"
    )

def register_signal(d: dict):
    x = dict(d)
    x["ts"] = now_ts().isoformat()
    DAILY_SIGNALS.append(x)

# ====================================================================
# LOOPS PRINCIPALES
# ====================================================================
async def monitor_loop():
    await send_tg(
        f"🤖 <b>{PROJECT_NAME}</b> iniciado.\n"
        f"Modo actual: <b>{MODE['current'].upper()}</b>\n"
        f"Exchanges de datos: <code>{', '.join(EXCHANGES)}</code>\n"
        f"Operando 24/7 en mercados de criptomonedas."
    )

    await send_tg("🧠 Iniciando descarga y entrenamiento de pares… puede tardar unos minutos la primera vez.")
    await download_all_pares()

    global APROBADOS
    APROBADOS = await filtra_aprobados()

    if not APROBADOS:
        await send_tg(
            "❌ Ningún par cumple el WR/trades mínimos para el modo actual.\n"
            "Puedes ajustar la lógica o reducir filtros si quieres más señales."
        )
        return

    await send_tg("🚀 Comenzando escaneo en vivo SOLO de pares aprobados…")

    while True:
        ensure_daily_reset()

        if not MONITOR_ACTIVE:
            await asyncio.sleep(3)
            continue

        for sym in APROBADOS:
            try:
                res = analiza_vivo(sym)
                if not res:
                    continue

                day_key = (sym, res["side"], now_ts().date())
                if STATE["daily_count"][day_key] >= 2:
                    continue

                key = (sym, res["side"])
                if (now_ts().timestamp() - STATE["last_sent"].get(key, 0)) < 30 * 60:
                    continue

                msg = build_alert(
                    sym,
                    res["side"],
                    res["price"],
                    res["sl"],
                    res["tp1"],
                    res["tp2"],
                    res["rr"],
                    res.get("score")
                )
                await send_tg(msg)

                register_signal({
                    "symbol": sym,
                    "side": res["side"],
                    "price": res["price"],
                    "sl": res["sl"],
                    "tp1": res["tp1"],
                    "tp2": res["tp2"],
                    "rr": res["rr"],
                    "score": res.get("score")
                })

                STATE["last_sent"][key] = now_ts().timestamp()
                STATE["daily_count"][day_key] += 1

            except Exception as e:
                print(f"⚠️ Error en análisis {sym}: {e}")

        await asyncio.sleep(RUN_EVERY_SEC)

# ====================================================================
# FASTAPI (keep-alive)
# ====================================================================
app = FastAPI()

@app.get("/ping")
async def ping():
    return {"ok": True, "service": PROJECT_NAME, "time": now_ts().isoformat()}

def start_http():
    th = threading.Thread(
        target=lambda: uvicorn.run(
            app,
            host="0.0.0.0",
            port=int(os.environ.get("PORT", "8080")),
            log_level="warning"
        ),
        daemon=True
    )
    th.start()

# ====================================================================
# ENTRYPOINT
# ====================================================================
async def main_async():
    start_http()
    await monitor_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except RuntimeError:
        loop = asyncio.get_event_loop()
        loop.create_task(main_async())
        loop.run_forever()
