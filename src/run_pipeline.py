import pandas as pd
import os
import glob
import logging
import subprocess  # Importante para ejecutar el segundo script
from config import PATH_INPUT, PATH_OUTPUT, PATH_REJECTED, LOG_DIR, LOG_FILE
from utils import limpiar_texto, obtener_metricas_y_duplicados, reparar_encoding

# Configuración de carpetas y logs
os.makedirs(PATH_OUTPUT, exist_ok=True)
os.makedirs(PATH_REJECTED, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def procesar_archivo(path_completo):
    nombre_original = os.path.basename(path_completo)
    try:
        # 1. LECTURA ROBUSTA
        try:
            df = pd.read_csv(path_completo, encoding='utf-8-sig', sep=None, engine='python')
        except:
            df = pd.read_csv(path_completo, encoding='latin-1', sep=None, engine='python')

        # 2. REPARACIÓN DE DATOS Y COLUMNAS
        df = df.map(reparar_encoding)
        df.columns = [limpiar_texto(col) for col in df.columns]

        # 3. AUDITORÍA DE CALIDAD (Duplicados)
        nulos, df_dups = obtener_metricas_y_duplicados(df)
        if not df_dups.empty:
            nombre_rechazados = f"duplicados_{nombre_original}"
            df_dups.to_csv(os.path.join(PATH_REJECTED, nombre_rechazados), index=False)
            logging.warning(f"📊 {nombre_original}: {len(df_dups)} duplicados movidos a /rejected.")

        # 4. LIMPIEZA FINAL
        df = df.drop_duplicates(keep='first')
        df = df.fillna({
            col: 0 if df[col].dtype in ['int64', 'float64'] else 'desconocido' 
            for col in df.columns
        })
        
        # 5. GUARDADO
        nombre_final = limpiar_texto(nombre_original.replace(".csv", "")) + "_limpio.csv"
        path_destino = os.path.join(PATH_OUTPUT, nombre_final)
        df.to_csv(path_destino, index=False)
        logging.info(f"✅ Éxito: {nombre_final} | Filas finales: {len(df)}")

    except Exception as e:
        logging.error(f"❌ Error crítico en {nombre_original}: {str(e)}")

def ejecutar_pipeline():
    logging.info("🚀 --- INICIANDO PIPELINE GLOBAL ---")
    
    # --- FASE 1: INGESTA Y TRANSFORMACIÓN ---
    patron = os.path.join(PATH_INPUT, "*.csv")
    archivos = glob.glob(patron)
    
    if not archivos:
        logging.error(f"❌ No se encontraron CSVs en {PATH_INPUT}")
        return

    for archivo in archivos:
        procesar_archivo(archivo)
    
    logging.info("✔ Ingesta y Transformación completadas.")

    # --- FASE 2: CARGA A DUCKDB (LLAMADA AUTOMÁTICA) ---
    logging.info("⏳ Iniciando etapa de Carga a DuckDB...")
    try:
   
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        script_carga = os.path.join(directorio_actual, "cargar_duckdb.py")
        
        resultado = subprocess.run(
            ['python3', script_carga],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("✔ Transformación y Carga a DuckDB completada exitosamente.")
        logging.info("🏁 --- PIPELINE FINALIZADO CON ÉXITO ---")
        
    except subprocess.CalledProcessError as e:
        logging.error("❌ Error durante la carga a DuckDB:")
        logging.error(e.stderr)
    except Exception as e:
        logging.error(f"❌ Error inesperado al llamar a DuckDB: {str(e)}")

if __name__ == "__main__":
    ejecutar_pipeline()