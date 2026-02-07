import os
import glob
import logging
import subprocess
import pandas as pd
from config import PATH_INPUT, PATH_OUTPUT, LOG_FILE
from utils import limpiar_texto, reparar_encoding, obtener_metricas_y_duplicados
from validaciones import validate_data
from check_referencial import verificar_integridad

# Configuración centralizada de Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def ejecutar_pipeline():
    logging.info("🚀 --- INICIANDO PIPELINE---")

    # Identifiacion de archivos a procesar 
    archivos = glob.glob(os.path.join(PATH_INPUT, "*.csv"))
    if not archivos:
        logging.error(f"❌ No se encontraron archivos en {PATH_INPUT}")
        return

    # Procesamiento e ingesta de cada archivo
    for ruta_archivo in archivos:
        nombre_archivo = os.path.basename(ruta_archivo)
        logging.info(f"📄 Procesando: {nombre_archivo}")

        try:
            # Lectura del CSV con manejo de encoding y delimitadores variados
            df = pd.read_csv(ruta_archivo, encoding='utf-8-sig', sep=None, engine='python')
            
            # Limpieza llamando a Utils 
            df = df.map(reparar_encoding)
            df.columns = [limpiar_texto(col) for col in df.columns]
            
            
            # Si validate_data retorna False, abortamos el pipeline
            if not validate_data(df, nombre_archivo):
                logging.critical(f"🛑 Error de validación crítica en {nombre_archivo}. ABORTANDO PIPELINE.")
                return 

            # Guardado de archivo procesado
            nombre_limpio = limpiar_texto(nombre_archivo.replace(".csv", "")) + "_limpio.csv"
            df.to_csv(os.path.join(PATH_OUTPUT, nombre_limpio), index=False)
            logging.info(f"✅ Archivo '{nombre_limpio}' listo para DuckDB.")

        except Exception as e:
            logging.error(f"❌ Error inesperado al procesar {nombre_archivo}: {e}")
            return

    # Carga a duckdb y creación del modelo estrella
    logging.info("⏳ Iniciando carga y transformación en DuckDB...")
    try:
        script_carga = os.path.join(os.path.dirname(__file__), "cargar_duckdb.py")
        subprocess.run(['python3', script_carga], check=True)
        logging.info("✔ Carga y Modelo Estrella completados.")
    except subprocess.CalledProcessError:
        logging.error("❌ Falló la ejecución de cargar_duckdb.py")
        return

    # chequeo de integridad referencial
    if verificar_integridad():
        logging.info("🛡️ Integridad referencial verificada con éxito.")
        logging.info("🏁 --- PIPELINE FINALIZADO EXITOSAMENTE ---")
    else:
        logging.error("❌ El pipeline terminó pero se detectaron inconsistencias en la DB.")

if __name__ == "__main__":
    ejecutar_pipeline()