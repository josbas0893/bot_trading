import ccxt
import pandas as pd
import os
import ssl
import smtplib
from email.message import EmailMessage
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

# Variables desde Render
EXCHANGES = [s.strip() for s in os.getenv("EXCHANGES", "kucoin").split(",")]
MAX_PAIRS = int(os.getenv("MAX_PAIRS", "100"))
SMTP_EMAIL = os.getenv("SMTP_EMAIL", "")
SMTP_APP_PASSWORD = os.getenv("SMTP_APP_PASSWORD", "")
SMTP_TO = os.getenv("SMTP_TO", SMTP_EMAIL)
TIMEFRAMES = [tf.strip() for tf in os.getenv("TIMEFRAMES", "4h,1h,15m,5m").split(",")]

exchange_name = EXCHANGES[0]
exchange = getattr(ccxt, exchange_name)({
    "options": {"defaultType": "future"},
    "enableRateLimit": True
})
exchange.load_markets()
pairs = [s for s in exchange.symbols if "/USDT" in s and "PERP" in s][:MAX_PAIRS]

def descargar_archivos():
    archivos = []
    hoy = datetime.now(UTC).strftime("%Y-%m-%d")
    for pair in pairs:
        for tf in TIMEFRAMES:
            try:
                data = exchange.fetch_ohlcv(pair, tf, limit=1000)
                df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
                filename = f"{pair.replace('/','_').replace(':','_')}_{tf}_{hoy}.csv"
                df.to_csv(filename, index=False)
                archivos.append(filename)
                print(f"Guardado: {filename}")
            except Exception as e:
                print(f"Error con {pair} {tf}: {e}")
    return archivos

def enviar_gmail(archivos):
    if not (SMTP_EMAIL and SMTP_APP_PASSWORD and SMTP_TO):
        print("SMTP no configurado; salto envío.")
        return
    msg = EmailMessage()
    msg["From"] = SMTP_EMAIL
    msg["To"] = SMTP_TO
    msg["Subject"] = f"Archivos futuros {exchange_name} {datetime.now(UTC).strftime('%Y-%m-%d')}"
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

if __name__ == "__main__":
    archivos = descargar_archivos()
    enviar_gmail(archivos)
    limpiar_archivos(archivos)
