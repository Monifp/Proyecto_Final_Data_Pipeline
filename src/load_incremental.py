import pandas as pd
import os
import logging
import duckdb
from config import PATH_INPUT, PATH_OUTPUT, DB_PATH
from utils import setup_logging
from validaciones import validate_data

def normalizar_columnas(df):
    """
    Estandariza los nombres de las columnas para evitar duplicados por 
    tildes, mayúsculas o errores de codificación de Excel.
    """
     
    df.columns = df.columns.str.lower().str.strip()
    
    replacements = {
        'método_pago': 'metodo_pago',
        'm√©todo_pago': 'metodo_pago',
        'mâˆšÂ©todo_pago': 'metodo_pago',
        'ï»¿id_venta': 'id_venta',
        'id_venta': 'id_venta'
    }
    for old, new in replacements.items():
        df.columns = [c.replace(old, new) for c in df.columns]
        
    return df

def actualizar_base_datos(con):
    """
    Sincroniza las tablas de DuckDB asegurando que los IDs coincidan 
    sin importar si son texto, números o tienen decimales (.0).
    """
    logging.info("💾 Sincronizando DuckDB con lógica de vinculación robusta...")
    
    csv_clientes = os.path.join(PATH_OUTPUT, 'clientes_limpio.csv')
    csv_productos = os.path.join(PATH_OUTPUT, 'productos_limpio.csv')
    csv_ventas = os.path.join(PATH_OUTPUT, 'ventas_limpio.csv')

    # Dimensión Clientes
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_clientes AS 
        SELECT row_number() OVER () AS sk_cliente, * FROM (SELECT DISTINCT * FROM read_csv_auto('{csv_clientes}', all_varchar=True));
    """)

    # Dimensión Productos  
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_productos AS 
        SELECT 
            row_number() OVER () AS sk_producto, 
            * EXCLUDE (precio_unitario, id_producto),
            TRIM(id_producto) AS id_producto,
            replace(precio_unitario, ',', '.')::DOUBLE AS precio_unitario
        FROM (SELECT DISTINCT * FROM read_csv_auto('{csv_productos}', all_varchar=True));
    """)

    # Dimensión Métodos de Pago 
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_metodos_pago AS
        SELECT row_number() OVER () AS sk_metodo_pago, metodo
        FROM (SELECT DISTINCT metodo_pago AS metodo FROM read_csv_auto('{csv_ventas}', all_varchar=True));
    """)

    # Tabla de Hechos  
    con.execute(f"""
        CREATE OR REPLACE TABLE fct_ventas AS 
        SELECT 
            v.* EXCLUDE (fecha, cantidad, metodo_pago, id_producto, id_cliente),
            strptime(v.fecha::VARCHAR, '%d/%m/%Y')::DATE AS fecha_id, 
            c.sk_cliente, 
            p.sk_producto, 
            mp.sk_metodo_pago,
            v.cantidad::INT AS cantidad,
            (v.cantidad::INT * p.precio_unitario) AS monto_total
        FROM read_csv_auto('{csv_ventas}', all_varchar=True) v
        LEFT JOIN dim_productos p ON 
            TRY_CAST(TRIM(v.id_producto) AS BIGINT) = TRY_CAST(TRIM(p.id_producto) AS BIGINT)
        LEFT JOIN dim_clientes c ON 
            TRY_CAST(TRIM(v.id_cliente) AS BIGINT) = TRY_CAST(TRIM(c.id_cliente) AS BIGINT)
        LEFT JOIN dim_metodos_pago mp ON 
            TRIM(v.metodo_pago) = TRIM(mp.metodo);
    """)
    
    # Verificación de integridad
    nulos = con.execute("SELECT COUNT(*) FROM fct_ventas WHERE sk_producto IS NULL").fetchone()[0]
    if nulos > 0:
        logging.warning(f"⚠️ Se detectaron {nulos} ventas sin producto vinculado (ID no encontrado).")
    else:
        logging.info("✅ Vinculación exitosa: Todas las ventas tienen su SK asignado.")

def procesar_nuevos_datos():

    """ Función principal para procesar nuevos datos de ventas, validarlos, 
    actualizar el archivo maestro y sincronizar con DuckDB.
    """

    setup_logging()
    logging.info("🚀 Iniciando proceso de Ingesta Incremental...")

    input_path = os.path.join(PATH_INPUT, "new_data", "ventas_nuevas.csv")
    output_path = os.path.join(PATH_OUTPUT, "ventas_limpio.csv")

    if not os.path.exists(input_path):
        logging.warning(f"⚠️ No se encontró archivo nuevo en: {input_path}")
        return

    try:
        
        df_nuevo = pd.read_csv(input_path, sep=None, engine='python', encoding='utf-8')
        df_nuevo = normalizar_columnas(df_nuevo)
        
         
        df_limpio_nuevo, _ = validate_data(df_nuevo, "ventas_nuevas.csv")

        if df_limpio_nuevo.empty:
            logging.error("❌ Los registros nuevos no superaron las validaciones.")
            return

        
        if os.path.exists(output_path):
            df_historico = pd.read_csv(output_path)
            df_historico = normalizar_columnas(df_historico)
            
             
            df_final = pd.concat([df_historico, df_limpio_nuevo], ignore_index=True)
            df_final = df_final.drop_duplicates()
        else:
            df_final = df_limpio_nuevo

        
        df_final.to_csv(output_path, index=False, encoding='utf-8')
        logging.info(f"✅ Archivo maestro actualizado. Registros totales: {len(df_final)}")

        # Actualizar base de datos DuckDB
        con = duckdb.connect(DB_PATH)
        actualizar_base_datos(con)
        con.close()
        
        logging.info("🏁 Fin del proceso incremental.")

    except Exception as e:
        logging.error(f"❌ Error crítico en la ingesta: {e}")

if __name__ == "__main__":
    procesar_nuevos_datos()