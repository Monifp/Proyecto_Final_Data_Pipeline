import duckdb
import os
import time
import logging
import pandas as pd
from datetime import datetime
from config import DB_PATH, PATH_OUTPUT, LOG_DIR
from validaciones import validate_data

LOG_LOAD = os.path.join(LOG_DIR, "load_incremental.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_LOAD, mode='a', encoding='utf-8'), logging.StreamHandler()]
)

def ejecutar_carga_validada():
    con = duckdb.connect(DB_PATH)
    start_time = time.time()
    archivo_origen = os.path.join(PATH_OUTPUT, "ventas2_limpio.csv")
    
    logging.info(f"🚀 Iniciando CARGA INCREMENTAL CON TRANSFORMACIÓN: {archivo_origen}")

    try:
        if not os.path.exists(archivo_origen):
            logging.error(f"❌ No se encontró: {archivo_origen}")
            return

        # 1. LEER DATOS
        df = pd.read_csv(archivo_origen)

        # 2. MAPEADO DE COLUMNAS
        mapeo = {
            'fecha': 'fecha_id',
            'm√©todo_pago': 'sk_metodo_pago',
            'metodo_pago': 'sk_metodo_pago',
            'id_cliente': 'sk_cliente',
            'id_producto': 'sk_producto',
            'cliente_id': 'sk_cliente',
            'producto_id': 'sk_producto'
        }
        df = df.rename(columns={k: v for k, v in mapeo.items() if k in df.columns})

        # --- AQUÍ LA SOLUCIÓN AL ERROR DE FECHA ---
        # Convertimos la columna al formato que DuckDB espera (YYYY-MM-DD)
        if 'fecha_id' in df.columns:
            logging.info("📅 Transformando formato de fecha a ISO (YYYY-MM-DD)...")
            # dayfirst=True es vital para que entienda que 05/05 es día/mes
            df['fecha_id'] = pd.to_datetime(df['fecha_id'], dayfirst=True).dt.strftime('%Y-%m-%d')
        # ------------------------------------------

        # 3. VALIDACIÓN DE NEGOCIO
        if not validate_data(df, "ventas2_limpio.csv"):
            logging.critical("🛑 VALIDACIÓN FALLIDA: Los datos no son aptos.")
            return

        # 4. INSERCIÓN INCREMENTAL
        columnas_finales = ", ".join(df.columns)
        con.execute(f"INSERT INTO fct_ventas ({columnas_finales}) SELECT {columnas_finales} FROM df")
        
        # 5. MÉTRICAS
        duracion = round(time.time() - start_time, 4)
        registros_nuevos = len(df)
        total_db = con.execute("SELECT COUNT(*) FROM fct_ventas").fetchone()[0]
        
        logging.info(f"✅ ÉXITO: {registros_nuevos} registros sumados. Total DB: {total_db}")

    except Exception as e:
        logging.error(f"❌ Error fatal: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    ejecutar_carga_validada()