import os
import glob
import logging
import subprocess
import pandas as pd
from config import PATH_INPUT, PATH_OUTPUT, LOG_FILE
from utils import limpiar_texto, reparar_encoding
from validaciones import validate_data
from check_referencial import verificar_integridad

# Configuración de Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def ejecutar_pipeline():
    logging.info("🚀 --- INICIANDO PIPELINE ---")

    # 1. IDENTIFICACIÓN DE ARCHIVOS
    archivos = glob.glob(os.path.join(PATH_INPUT, "*.csv"))
    if not archivos:
        logging.error(f"❌ No se encontraron archivos en {PATH_INPUT}")
        return

    for ruta_archivo in archivos:
        nombre_archivo = os.path.basename(ruta_archivo)
        logging.info(f"📄 Procesando: {nombre_archivo}")

        try:
            # Lectura del archivo original
            df = pd.read_csv(ruta_archivo, encoding='utf-8-sig', sep=None, engine='python')
            
            # Limpieza técnica inicial (Utils)
            df = df.map(reparar_encoding)
            df.columns = [limpiar_texto(col) for col in df.columns]
            
            # 2. VALIDACIÓN Y FILTRADO (Separación de registros buenos y malos)
            # validate_data devuelve (df_buenos, df_malos)
            df_buenos, df_malos = validate_data(df, nombre_archivo)
            
            if df_buenos.empty:
                logging.warning(f"⚠️ El archivo {nombre_archivo} no contiene registros válidos. Saltando...")
                continue

            # Guardado de archivo procesado (Listo para DuckDB)
            nombre_limpio = limpiar_texto(nombre_archivo.replace(".csv", "")) + "_limpio.csv"
            path_guardado = os.path.join(PATH_OUTPUT, nombre_limpio)
            df_buenos.to_csv(path_guardado, index=False)
            logging.info(f"✅ Archivo '{nombre_limpio}' generado con {len(df_buenos)} registros.")

        except Exception as e:
            logging.error(f"❌ Error inesperado al procesar {nombre_archivo}: {e}")
            return

    # 3. CARGA A DUCKDB
    logging.info("⏳ Iniciando carga en DuckDB...")
    try:
        # Ejecuta el script de carga (asegúrate de que cargar_duckdb.py use los archivos de PATH_OUTPUT)
        subprocess.run(['python3', 'cargar_duckdb.py'], check=True)
        logging.info("✔ Carga al Modelo Estrella completada.")
    except Exception as e:
        logging.error(f"❌ Falló la carga a la base de datos: {e}")
        return

    # 4. CHEQUEO DE INTEGRIDAD REFERENCIAL
    if verificar_integridad():
        logging.info("🛡️ Integridad referencial verificada con éxito.")
        logging.info("🏁 --- PIPELINE FINALIZADO EXITOSAMENTE ---")
    else:
        logging.error("❌ Se detectaron inconsistencias en la base de datos.")

if __name__ == "__main__":
    ejecutar_pipeline()