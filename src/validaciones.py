import pandas as pd
import logging
import os
from datetime import datetime

def validate_data(df, nombre_archivo):
    logging.info(f"🔍 Validando reglas específicas para: {nombre_archivo}")
    
    # 1. Definimos las reglas según el nombre del archivo
    # Usamos .lower() para que sea insensible a mayúsculas
    nombre = nombre_archivo.lower()
    
    mask_valid = pd.Series([True] * len(df)) # Por defecto, todo es válido

    try:
        # --- REGLAS PARA VENTAS ---
        if 'ventas' in nombre:
            col_id = next((c for c in df.columns if 'id_venta' in c.lower() or 'venta_id' in c.lower()), None)
            col_cant = next((c for c in df.columns if 'cant' in c.lower()), None)
            
            if col_id and col_cant:
                df[col_cant] = pd.to_numeric(df[col_cant], errors='coerce')
                mask_valid = df[col_id].notnull() & (df[col_cant] > 0)
            else:
                logging.warning(f"⚠️ Faltan columnas críticas en archivo de ventas: {nombre_archivo}")

        # --- REGLAS PARA PRODUCTOS ---
        elif 'producto' in nombre:
            col_prod = next((c for c in df.columns if 'producto' in c.lower()), None)
            if col_prod:
                mask_valid = df[col_prod].notnull()

        # --- REGLAS PARA MÉTODOS DE PAGO / CLIENTES ---
        # En estos archivos, generalmente solo validamos que el ID no sea nulo
        else:
            col_id_general = df.columns[0] # Asumimos que la primera columna es el ID
            mask_valid = df[col_id_general].notnull()

        # 2. Separación de datos
        df_validos = df[mask_valid].copy()
        df_rechazados = df[~mask_valid].copy()
        
        # 3. Guardado de rechazados (solo si hay errores)
        if not df_rechazados.empty:
            folder_rejected = "data/rejected"
            os.makedirs(folder_rejected, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            path_rechazados = os.path.join(folder_rejected, f"rechazados_{nombre_archivo}_{timestamp}.csv")
            df_rechazados.to_csv(path_rechazados, index=False)
            logging.warning(f"⚠️ {len(df_rechazados)} registros rechazados en {nombre_archivo}")

        return df_validos, df_rechazados

    except Exception as e:
        logging.error(f"❌ Error validando {nombre_archivo}: {e}")
        return df, pd.DataFrame() # Ante la duda, dejamos pasar los datos para no frenar el flujo