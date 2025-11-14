import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
import ta
import glob

# Leer todos los CSV descargados de velas de 1h, por ejemplo
archivos = glob.glob("*_1h_*.csv")
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
print("Modelo guardado como modelo_rf.pkl")
