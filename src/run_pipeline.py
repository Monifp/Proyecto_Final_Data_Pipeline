import os, glob, logging, pandas as pd
from config import PATH_INPUT, PATH_OUTPUT
from utils import setup_logging, limpiar_texto, reparar_encoding
from validaciones import validate_data
from cargar_duckdb import ejecutar_modelo
from check_referencial import verificar_integridad

def ejecutar_pipeline():
    setup_logging()
    logging.info("🚀 --- INICIANDO PIPELINE: BRAIN & CODE ---")

    archivos = glob.glob(os.path.join(PATH_INPUT, "*.csv"))
    if not archivos:
        logging.error(f"❌ No se encontraron archivos en {PATH_INPUT}")
        return

    for ruta in archivos:
        nombre_original = os.path.basename(ruta)
        logging.info(f"📄 Procesando: {nombre_original}")
        
        try:
            df = pd.read_csv(ruta, sep=None, engine='python')

            # --- Informacion inicial del DataFrame ---
            print(f"\n--- INFO: {nombre_original} ---")
            df.info()
            print(f"--- HEAD: {nombre_original} ---")
            print(df.head(3))
            print("-" * 30)

            # Transformación
            df = df.map(reparar_encoding)
            df.columns = [limpiar_texto(col) for col in df.columns]
            
            # Validación y Auditoría (Duplicados/Nulos van a /rejected)
            df_buenos, _ = validate_data(df, nombre_original)
            
            if not df_buenos.empty:
                 
                nombre_base = nombre_original.replace(".csv", "")
                path_out = os.path.join(PATH_OUTPUT, f"{nombre_base}_limpio.csv")
                df_buenos.to_csv(path_out, index=False)
                logging.info(f"✅ Generado: {os.path.basename(path_out)}")

        except Exception as e:
            logging.error(f"❌ Falló {nombre_original}: {e}")

    # Ejecución de Modelado y Chequeo
    try:
        ejecutar_modelo()
        
        if verificar_integridad():
             
            print("\n" + "="*50)
            print("✨ PIPELINE FINALIZADO EXITOSAMENTE ✨")
            print("="*50)
            logging.info("🏁 Pipeline finalizado exitosamente.")
        else:
            logging.error("❌ El pipeline terminó con advertencias de integridad.")

    except Exception as e:
        logging.error(f"❌ Error en la fase final del pipeline: {e}")

if __name__ == "__main__":
    ejecutar_pipeline()