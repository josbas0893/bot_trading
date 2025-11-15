# ======================================================================
# Bot V4 – Render 24/7  (sin pandas, solo polars)
# 1. Descarga velas 4h/1h/15m/5m (Bitunix)
# 2. Back-test individual ≥ 65 % WR → lista blanca
# 3. Escanía y manda SEÑALES (solo aprobados)
# 4. No tradea automáticamente
# ======================================================================

import os, re, time, math, asyncio, threading, smtplib, ssl, io, pytz
from datetime import datetime, UTC, timedelta
from email.message import EmailMessage
from collections import defaultdict

import ccxt
import numpy as np
import polars as pl

from fastapi import FastAPI, Request
import uvicorn
from dotenv import load_dotenv

from telegram import Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

load_dotenv()

# ---------- CONFIG ----------
EXCHANGES           = ["bitunix"]
MAX_PAIRS           = int(os.getenv("MAX_PAIRS", "150"))
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
SMTP_EMAIL          = os.getenv("SMTP_EMAIL", "")
SMTP_APP_PASSWORD   = os.getenv("SMTP_APP_PASSWORD", "")
SMTP_TO             = os.getenv("SMTP_TO", SMTP_EMAIL)
PROJECT_NAME        = os.getenv("PROJECT_NAME", "BotV4")
RUN_EVERY_SEC       = int(os.getenv("RUN_EVERY_SEC", "300"))
DATA_DIR            = "data"
BACKTEST_DAYS       = 60
WR_MINIMO           = 0.65
R_COMISION          = 0.0008
R_SLIPPAGE          = 0.0005

MODE                = {"current": "normal"}
MONITOR_ACTIVE      = True
STATE               = {"last_sent": {}, "daily_count": defaultdict(int), "last_reset": datetime.now(UTC).date()}
DAILY_SIGNALS       = []

# ====================================================================
# 1. EXCHANGE → solo Bitunix
# ====================================================================
EX_OBJ = None
def init_exchange():
    global EX_OBJ
    try:
        ex = ccxt.bitunix({"enableRateLimit": True})
        ex.load_markets()
        EX_OBJ = ex
        print(f"✅ Bitunix con {len(ex.symbols)} símbolos")
    except Exception as e:
        print("⚠️ No se pudo iniciar Bitunix:", e)
init_exchange()

# ====================================================================
# 2. UTILS / TELEGRAM
# ====================================================================
def now_ts(): return datetime.now(UTC)

async def send_tg(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        print("❌ Telegram:", e)

# ====================================================================
# 3. DESCARGA DE VELAS
# ====================================================================
def csv_path(symbol, tf):
    return f"{DATA_DIR}/{tf}/{symbol.replace('/', '_')}_{tf}.csv"

def ensure_dirs():
    for tf in ["5m", "15m", "1h", "4h"]:
        os.makedirs(f"{DATA_DIR}/{tf}", exist_ok=True)

def download_once(symbol, tf, limit=1500):
    if not EX_OBJ:
        return pl.DataFrame()
    try:
        data = EX_OBJ.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        df = pl.DataFrame(data, schema=["ts", "open", "high", "low", "close", "volume"])
        df = df.with_columns(pl.col("ts").cast(pl.Int64))
        df.write_csv(csv_path(symbol, tf))
        return df
    except Exception as e:
        print(f"⚠️ download {symbol} {tf}: {e}")
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
        f"📥 Descarga completa Bitunix\n"
        f"🗂️  Pares: {len(pairs)}\n"
        f"📊 Filas: {total_rows/1e6:.1f} M\n"
        f"⏱️ Tiempo: {elapsed/60:.1f} min"
    )
    return pairs

# ====================================================================
# 4. CONSTRUCCIÓN DE PARES (solo USDT Bitunix)
# ====================================================================
def build_pairs_list():
    if not EX_OBJ:
        return []
    syms = [s for s in EX_OBJ.symbols if s.endswith("/USDT")]
    syms = [s for s in syms if not re.match(r"^(1000|1M|10K|B-|.*-).*?/USDT$", s)]
    return sorted(syms)[:MAX_PAIRS]

# ====================================================================
# 5. BACK-TEST INDIVIDUAL (con polars)
# ====================================================================
def backtest_pare(symbol):
    df5 = load_or_download(symbol, "5m")
    if len(df5) < BACKTEST_DAYS * 288:
        return {"trades": 0, "wr": 0, "df": pl.DataFrame()}

    # indicadores
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
        c   = df5["close"][i]
        ema12= df5["EMA_12"][i]
        ema50= df5["EMA_50"][i]
        ema200=df5["EMA_200"][i]
        rsi = df5["RSI"][i]
        if pl.is_nan(c): continue
        is_long = None
        if c > ema200*0.998 and ema12 > ema50*0.998 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
            is_long = True
        elif c < ema200*1.002 and ema12 < ema50*1.002 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
            is_long = False
        if is_long is None:
            continue
        # sl/tp
        atr = (df5["high"][i] - df5["low"][i]).rolling_mean(14)[i]
        if pl.is_nan(atr): continue
        sl  = c - atr*1.8 if is_long else c + atr*1.8
        tp1 = c + atr*1.8*1.5 if is_long else c - atr*1.8*1.5
        fut = df5[i+1:i+21]
        if len(fut) < 2: continue
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
    df_trades = pl.DataFrame({"R": trades})
    wr = (df_trades["R"] > 0).mean() if len(df_trades) else 0
    return {"trades": len(df_trades), "wr": wr, "df": df_trades}

async def filtra_aprobados():
    pairs = build_pairs_list()
    aprobados = []
    txt = "✅ Estrategia válida (≥65 % WR)\n\n"
    for sym in pairs:
        res = backtest_pare(sym)
        if res["wr"] >= WR_MINIMO and res["trades"] >= 10:
            aprobados.append(sym)
            txt += f"• {sym}  –  {res['wr']:.0%} ({res['trades']} trades)\n"
    txt += f"\n💎 Total: {len(aprobados)} pares\n"
    await send_tg(txt)
    return aprobados

# ====================================================================
# 6. INDICADORES (en vivo) con polars
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
# 7. ANÁLISIS EN VIVO (solo pares aprobados) con polars
# ====================================================================
APROBADOS = []

def analiza_vivo(symbol: str):
    df5 = load_or_download(symbol, "5m")
    if len(df5) < 60:
        return None
    df5 = compute_indicators(df5)
    last = df5.tail(1)
    c   = last["close"][0]
    ema12= last["EMA_12"][0]
    ema50= last["EMA_50"][0]
    ema200=last["EMA_200"][0]
    rsi = last["RSI"][0]
    if pl.is_nan(c): return None
    is_long = None
    if c > ema200*0.998 and ema12 > ema50*0.998 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
        is_long = True
    elif c < ema200*1.002 and ema12 < ema50*1.002 and 35 <= rsi <= 65 and abs(c-ema50)/ema50 <= 0.006:
        is_long = False
    if is_long is None:
        return None
    atr = (last["high"][0] - last["low"][0])
    if pl.is_nan(atr): return None
    sl  = c - atr*1.8 if is_long else c + atr*1.8
    tp1 = c + atr*1.8*1.5 if is_long else c - atr*1.8*1.5
    tp2 = c + atr*1.8*2.5 if is_long else c - atr*1.8*2.5
    rr  = abs(tp1 - c) / abs(c - sl)
    if rr < 1.6:
        return None
    side = "LONG" if is_long else "SHORT"
    return {"side": side, "price": float(c), "sl": float(sl), "tp1": float(tp1), "tp2": float(tp2), "rr": float(rr)}

# ====================================================================
# 8. ALERTA / REGISTRO
# ====================================================================
def fmt_price(x):
    try:
        return f"{x:.6f}" if x < 1 else (f"{x:.4f}" if x < 100 else f"{x:.2f}")
    except:
        return str(x)

def build_alert(symbol, side, price, sl, tp1, tp2, rr):
    dir_emo = "🟢 LONG 📈" if side == "LONG" else "🔴 SHORT 📉"
    return (
        f"✨ <b>SEÑAL {PROJECT_NAME}</b> ✨\n\n"
        f"💰 <b>Par:</b> {symbol}\n"
        f"➡️ <b>Dirección:</b> {dir_emo}\n"
        f"📊 <b>Entry:</b> <code>{fmt_price(price)}</code>\n"
        f"🛑 <b>SL:</b> <code>{fmt_price(sl)}</code>\n"
        f"🎯 <b>TP1:</b> <code>{fmt_price(tp1)}</code>\n"
        f"🎯 <b>TP2:</b> <code>{fmt_price(tp2)}</code>\n"
        f"📈 <b>RR:</b> {rr:.2f}\n\n"
        f"⚠️ <b>Gestión:</b> mover SL a BE en TP1. Opera bajo tu riesgo."
    )

DAILY_SIGNALS = []
def register_signal(d: dict):
    x = dict(d)
    x["ts"] = now_ts().isoformat()
    DAILY_SIGNALS.append(x)

# ====================================================================
# 9. LOOPS PRINCIPALES
# ====================================================================
async def monitor_loop():
    # 1. descarga
    await download_all_pares()
    # 2. back-test
    global APROBADOS
    APROBADOS = await filtra_aprobados()
    if not APROBADOS:
        await send_tg("❌ Ningún par ≥ 65 % WR. Bot detenido.")
        return
    # 3. empieza a escanear
    await send_tg("🚀 Comenzando escaneo solo de pares aprobados…")
    while True:
        if not MONITOR_ACTIVE:
            await asyncio.sleep(3)
            continue
        for sym in APROBADOS:
            try:
                res = analiza_vivo(sym)
                if not res:
                    continue
                key = (sym, res["side"])
                if (now_ts().timestamp() - STATE["last_sent"].get(key, 0)) < 30 * 60:
                    continue
                msg = build_alert(sym, res["side"], res["price"], res["sl"], res["tp1"], res["tp2"], res["rr"])
                await send_tg(msg)
                register_signal({
                    "symbol": sym,
                    "side": res["side"],
                    "price": res["price"],
                    "sl": res["sl"],
                    "tp1": res["tp1"],
                    "tp2": res["tp2"],
                    "rr": res["rr"]
                })
                STATE["last_sent"][key] = now_ts().timestamp()
            except Exception as e:
                print(f"⚠️ Error {sym}: {e}")
        await asyncio.sleep(RUN_EVERY_SEC)

# ====================================================================
# 10. FASTAPI (keep-alive)
# ====================================================================
app = FastAPI()
@app.get("/ping")
async def ping():
    return {"ok": True, "service": PROJECT_NAME, "time": now_ts().isoformat()}

def start_http():
    th = threading.Thread(target=lambda: uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), log_level="warning"), daemon=True)
    th.start()

# ====================================================================
# 11. ENTRYPOINT
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
