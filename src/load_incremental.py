import duckdb
import pandas as pd
import logging
import os
from config import DB_PATH
from utils import setup_logging, limpiar_texto, reparar_encoding
from validaciones import validate_data

def agregar_datos_incremental(path_csv):
    setup_logging()
    logging.info(f"📥 Iniciando CARGA INCREMENTAL: {path_csv}")
    
    df = pd.read_csv(path_csv)
    
   

    # Procesamiento
    df = df.map(reparar_encoding)
    df.columns = [limpiar_texto(col) for col in df.columns]
    df_buenos, _ = validate_data(df, os.path.basename(path_csv))

    if not df_buenos.empty:
        con = duckdb.connect(DB_PATH)
        con.execute("INSERT INTO fct_ventas SELECT * FROM df_buenos")
        logging.info(f"✅ Se agregaron {len(df_buenos)} registros a la DB.")
        con.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1: agregar_datos_incremental(sys.argv[1])