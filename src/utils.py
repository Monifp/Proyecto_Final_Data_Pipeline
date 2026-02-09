import unicodedata
import pandas as pd
import logging
import os
from datetime import datetime
from config import LOG_FILE, PATH_REJECTED

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'), logging.StreamHandler()]
    )

def reparar_encoding(texto):
    if not isinstance(texto, str): return texto
    try: return texto.encode('latin-1').decode('utf-8')
    except: return texto

def limpiar_texto(texto):
    if not isinstance(texto, str): return texto
    texto = reparar_encoding(texto).lower().strip()
    texto_norm = unicodedata.normalize('NFKD', texto)
    solo_base = "".join([c for c in texto_norm if not unicodedata.combining(c)])
    return solo_base.replace(" ", "_").replace("-", "_")

def registrar_rechazos(df_rechazados, nombre_archivo, motivo):
    if not df_rechazados.empty:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = os.path.join(PATH_REJECTED, f"rechazo_{motivo}_{ts}_{nombre_archivo}")
        df_rechazados.to_csv(path, index=False)
        logging.warning(f"🚨 Registros rechazados ({motivo}) guardados en: {path}")