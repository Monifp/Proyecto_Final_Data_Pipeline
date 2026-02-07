import unicodedata
import pandas as pd

def reparar_encoding(texto):
    """
    Detecta y corrige caracteres rotos (Mojibake) como Ã¡ -> á.
    """
    if not isinstance(texto, str):
        return texto
    try:
        # Intenta revertir el error de interpretación latin-1/utf-8
        return texto.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # Si da error, es porque el texto ya estaba bien o es irreparable
        return texto

def limpiar_texto(texto):
    """
    Estandariza nombres para bases de datos: minúsculas, sin acentos y sin espacios.
    """
    if not isinstance(texto, str):
        return texto
    
    # 1. Reparamos posibles caracteres rotos antes de limpiar
    texto = reparar_encoding(texto)
    
    # 2. Normalizamos (quitar acentos y llevar a minúsculas)
    texto = texto.lower().strip()
    texto_norm = unicodedata.normalize('NFKD', texto)
    solo_base = "".join([c for c in texto_norm if not unicodedata.combining(c)])
    
    # 3. Formato final para SQL (snake_case)
    return solo_base.replace(" ", "_").replace("-", "_")

def obtener_metricas_y_duplicados(df):
    """
    Calcula nulos y separa filas duplicadas para auditoría.
    """
    nulos_totales = df.isnull().sum().sum()
    es_duplicado = df.duplicated(keep='first')
    df_duplicados = df[es_duplicado]
    
    return nulos_totales, df_duplicados