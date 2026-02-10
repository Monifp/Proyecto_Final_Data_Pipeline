import duckdb
import os
import logging
from config import DB_PATH, PATH_SCRIPTS, LOG_FILE

# Configuración de logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def ejecutar_modelo():
    con = None
    try:
        # Conexión a la base de datos usando la ruta de config.py
        logging.info(f"🔌 Conectando a DuckDB en: {DB_PATH}")
        con = duckdb.connect(DB_PATH)

        # Definir ruta del script SQL
        
        sql_path = os.path.join(PATH_SCRIPTS, "crear_modelo.sql")
        
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"No se encontró el archivo SQL en: {sql_path}")

        # Leer y ejecutar el script SQL
        logging.info(f"📜 Ejecutando script de modelado: {sql_path}")
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
         
        con.execute(sql_script)
        
        logging.info("✨ Modelo de datos creado exitosamente en DuckDB.")

    except Exception as e:
        logging.error(f"❌ Error durante la carga a DuckDB: {str(e)}")
         
        raise e
    
    finally:
        if con:
            con.close()
            logging.info("🔒 Conexión a DuckDB cerrada.")

if __name__ == "__main__":
    ejecutar_modelo()