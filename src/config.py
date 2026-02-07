import os

# Ruta del archivo config.py (esta en /src)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# SUBIMOS UN NIVEL para llegar a la raíz del proyecto (Proyecto_Final)
BASE_DIR = os.path.dirname(CURRENT_DIR)

# Ahora las subcarpetas apuntarán correctamente a la raíz
PATH_INPUT = os.path.join(BASE_DIR, "data", "ingested")
PATH_OUTPUT = os.path.join(BASE_DIR, "data", "processed")
PATH_REJECTED = os.path.join(BASE_DIR, "data", "rejected")
PATH_SCRIPTS = os.path.join(BASE_DIR, "scripts")

# Logs y Base de Datos
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "run_pipeline.log")
DB_DIR = os.path.join(BASE_DIR, "data", "database")
DB_PATH = os.path.join(DB_DIR, "Proyecto_final.db")

# Crear carpetas si no existen
os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
 