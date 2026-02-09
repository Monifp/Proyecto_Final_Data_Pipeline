import os
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)

 
PATH_INPUT = os.path.join(BASE_DIR, "data", "ingested")
PATH_OUTPUT = os.path.join(BASE_DIR, "data", "processed")
PATH_REJECTED = os.path.join(BASE_DIR, "data", "rejected")
PATH_SCRIPTS = os.path.join(BASE_DIR, "scripts")

 
DB_PATH = os.path.join(BASE_DIR, "data", "database", "Proyecto_final.db")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "run_pipeline.log")
 
for folder in [PATH_OUTPUT, PATH_REJECTED, LOG_DIR, os.path.dirname(DB_PATH)]:
    os.makedirs(folder, exist_ok=True)
 