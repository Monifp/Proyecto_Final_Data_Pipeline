import duckdb
import logging
import os
from config import DB_PATH, LOG_FILE

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
    errores_found = False
    
    logging.info("🛡️ Iniciando chequeo de integridad referencial...")

    try:
        # 1. Verificar Ventas sin Clientes (Usando sk_cliente según el error)
        query_clientes = """
            SELECT COUNT(*) 
            FROM fct_ventas f
            LEFT JOIN dim_clientes c ON f.sk_cliente = c.sk_cliente
            WHERE c.sk_cliente IS NULL
        """
        huerfanos_clientes = con.execute(query_clientes).fetchone()[0]
        
        if huerfanos_clientes > 0:
            logging.error(f"❌ INTEGRIDAD FALLIDA: {huerfanos_clientes} ventas con sk_cliente inexistente.")
            errores_found = True
        else:
            logging.info("✅ Integridad de Clientes: OK.")

        # 2. Verificar Ventas sin Productos (Ajustá sk_producto si se llama así también)
        # Si el error persiste con productos, revisá si es 'sk_producto' o 'id_producto'
        query_productos = """
            SELECT COUNT(*) 
            FROM fct_ventas f
            LEFT JOIN dim_productos p ON f.sk_producto = p.sk_producto
            WHERE p.sk_producto IS NULL
        """
        huerfanos_productos = con.execute(query_productos).fetchone()[0]
        
        if huerfanos_productos > 0:
            logging.error(f"❌ INTEGRIDAD FALLIDA: {huerfanos_productos} ventas con sk_producto inexistente.")
            errores_found = True
        else:
            logging.info("✅ Integridad de Productos: OK.")

    except Exception as e:
        logging.error(f"❌ Error durante la ejecución del check: {e}")
        errores_found = True
    finally:
        con.close()
        
    return not errores_found

if __name__ == "__main__":
    verificar_integridad()