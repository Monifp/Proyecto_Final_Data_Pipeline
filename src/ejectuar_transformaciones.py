import duckdb
import os
import logging
from config import DB_PATH, PATH_SCRIPTS, BASE_DIR
from utils import setup_logging

def transformar_datos():
    setup_logging()
    con = None
    try:
        # 1. Definir y crear carpeta de reportes
        path_reports = os.path.join(BASE_DIR, "data", "reports")
        os.makedirs(path_reports, exist_ok=True)
        
        logging.info("🛠️ Iniciando Proceso de Transformación y Reporte...")
        con = duckdb.connect(DB_PATH)

        # 2. Ejecutar el SQL de transformaciones
        sql_path = os.path.join(PATH_SCRIPTS, "transformaciones.sql")
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        con.execute(sql_script)
        
        # 3. PROCESAR Y EXPORTAR REPORTES
        reportes = {
            "ventas_por_categoria.csv": "SELECT * FROM reporte_ventas_categoria",
            "ventas_por_cliente.csv": "SELECT * FROM reporte_ventas_cliente"
        }

        print("\n" + "═"*60)
        print("📊 REPORTES GENERADOS".center(60))
        print("═"*60)

        for archivo, query in reportes.items():
            # Obtener datos
            df = con.execute(query).df()
            
            # Exportar a CSV
            full_path = os.path.join(path_reports, archivo)
            df.to_csv(full_path, index=False, sep=';', decimal=',')
            
            # Mostrar en terminal
            nombre_reporte = archivo.replace(".csv", "").replace("_", " ").upper()
            print(f"\n📈 {nombre_reporte}:")
            print(df.to_string(index=False))
            print(f"💾 Guardado en: {full_path}")

        # 4. CÁLCULO DE CONTROL FINAL
        res = con.execute("SELECT SUM(ingreso_total) FROM fct_ventas_enriquecida").fetchone()[0]
        ingreso = res if res is not None else 0
        
        print("\n" + "═"*60)
        print(f"💰 INGRESO TOTAL DEL PERIODO: ${ingreso:,.2f}".center(60))
        print("═"*60 + "\n")

    except Exception as e:
        logging.error(f"❌ Error en la transformación/reporte: {e}")
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    transformar_datos()