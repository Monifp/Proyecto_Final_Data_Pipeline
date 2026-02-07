import duckdb
import logging
from config import DB_PATH, LOG_FILE

# Configuramos el logging para que guarde en el mismo archivo del pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def verificar_integridad():
    con = duckdb.connect(DB_PATH)
    errores_encontrados = False
    
    logging.info("🛡️ Iniciando chequeo de integridad referencial...")

    # Verificar Ventas sin Clientes existentes
    query_clientes = """
        SELECT COUNT(*) 
        FROM fct_ventas f
        LEFT JOIN dim_clientes c ON f.cliente_id = c.cliente_id
        WHERE c.cliente_id IS NULL
    """
    huerfanos_clientes = con.execute(query_clientes).fetchone()[0]
    
    if huerfanos_clientes > 0:
        logging.error(f"❌ INTEGRIDAD FALLIDA: Hay {huerfanos_clientes} ventas vinculadas a clientes que NO existen en dim_clientes.")
        errores_encontrados = True
    else:
        logging.info("✅ Integridad de Clientes: OK.")

    # Verificar Ventas sin Productos existentes
    query_productos = """
        SELECT COUNT(*) 
        FROM fct_ventas f
        LEFT JOIN dim_productos p ON f.producto_id = p.producto_id
        WHERE p.producto_id IS NULL
    """
    huerfanos_productos = con.execute(query_productos).fetchone()[0]
    
    if huerfanos_productos > 0:
        logging.error(f"❌ INTEGRIDAD FALLIDA: Hay {huerfanos_productos} ventas vinculadas a productos que NO existen en dim_productos.")
        errores_encontrados = True
    else:
        logging.info("✅ Integridad de Productos: OK.")

    con.close()
    return not errores_encontrados

if __name__ == "__main__":
    if verificar_integridad():
        print("🚀 Todo en orden: La base de datos es consistente.")
    else:
        print("⚠️ Atención: Se encontraron inconsistencias. Revisar logs.")