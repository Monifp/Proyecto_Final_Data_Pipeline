import duckdb
import os
import time
import logging
import pandas as pd
from datetime import datetime
from config import DB_PATH, PATH_OUTPUT, LOG_DIR
from utils import limpiar_texto, reparar_encoding 
from validaciones import validate_data

# Configuración de log
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
    
    logging.info(f"🚀 Iniciando CARGA INCREMENTAL AUDITABLE: {archivo_origen}")

    try:
        if not os.path.exists(archivo_origen):
            logging.error(f"❌ No se encontró el archivo procesado: {archivo_origen}")
            return

        # 1. LECTURA Y LIMPIEZA TÉCNICA
        df = pd.read_csv(archivo_origen)
        df = df.map(reparar_encoding)
        df.columns = [limpiar_texto(col) for col in df.columns]

        # 2. MAPEADO HACIA MODELO ESTRELLA (Corrección de nombres y caracteres rotos)
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

        # 3. TRANSFORMACIÓN DE FECHA (ISO para DuckDB)
        if 'fecha_id' in df.columns:
            df['fecha_id'] = pd.to_datetime(df['fecha_id'], dayfirst=True).dt.strftime('%Y-%m-%d')

        # 4. VALIDACIÓN Y FILTRADO DE RECHAZADOS
        df_buenos, df_malos = validate_data(df, "ventas2_limpio.csv")
        
        if df_buenos.empty:
            logging.error("🛑 Sin registros válidos para cargar.")
            return

        # 5. INSERCIÓN INCREMENTAL DE REGISTROS VÁLIDOS
        columnas_finales = ", ".join(df_buenos.columns)
        con.execute(f"INSERT INTO fct_ventas ({columnas_finales}) SELECT {columnas_finales} FROM df_buenos")
        
        # 6. MÉTRICAS FINALES
        duracion = round(time.time() - start_time, 4)
        total_db = con.execute("SELECT COUNT(*) FROM fct_ventas").fetchone()[0]
        timestamp_carga = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Registro en Auditoría
        con.execute("CREATE TABLE IF NOT EXISTS control_cargas (proceso TEXT, fecha_ejecucion TEXT, registros INTEGER, duracion_seg FLOAT)")
        con.execute(f"INSERT INTO control_cargas VALUES ('Incremental Validated', '{timestamp_carga}', {len(df_buenos)}, {duracion})")

        logging.info(f"✅ CARGA EXITOSA: {len(df_buenos)} sumados | ❌ RECHAZADOS: {len(df_malos)}")
        logging.info(f"📊 Total en fct_ventas: {total_db} | Tiempo: {duracion}s")

    except Exception as e:
        logging.error(f"❌ Error fatal en el proceso: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    ejecutar_carga_validada()