import pandas as pd
import logging
from datetime import datetime
from utils import registrar_rechazos

def validate_data(df, nombre_archivo):
    logging.info(f"🔍 Validando calidad y rangos para: {nombre_archivo}")
    
    # 1. Detección de Duplicados
    duplicados = df[df.duplicated(keep='first')].copy()
    df_unicos = df.drop_duplicates(keep='first').copy()
    registrar_rechazos(duplicados, nombre_archivo, "duplicados")

    # 2. Validación de Negocio y Rangos
    nombre = nombre_archivo.lower()
    # Inicializamos la máscara (por defecto todo es válido)
    mask_valid = pd.Series([True] * len(df_unicos), index=df_unicos.index)

    if 'ventas' in nombre:
        # Buscamos columnas críticas
        col_id = next((c for c in df_unicos.columns if 'id_venta' in c.lower() or 'venta_id' in c.lower()), None)
        col_fecha = next((c for c in df_unicos.columns if 'fecha' in c.lower()), None)
        col_cant = next((c for c in df_unicos.columns if 'cant' in c.lower()), None)

        if col_id and col_fecha and col_cant:
            # A. Validación de Cantidad (Numérico > 0)
            cant_numerica = pd.to_numeric(df_unicos[col_cant], errors='coerce')
            mask_cant = (cant_numerica > 0)

            # B. Validación de Fechas (Rango 1980 - Hoy)
            # Convertimos a datetime (lo que no sea fecha será NaT)
            fechas_dt = pd.to_datetime(df_unicos[col_fecha], errors='coerce')
            
            fecha_min = datetime(1980, 1, 1)
            fecha_max = datetime.now()
            
            mask_fecha = (fechas_dt >= fecha_min) & (fechas_dt <= fecha_max)

            # C. Combinamos: ID no nulo Y Cantidad OK Y Fecha OK
            mask_valid = df_unicos[col_id].notnull() & mask_cant & mask_fecha
        else:
            logging.warning(f"⚠️ Faltan columnas clave para validar rangos en: {nombre_archivo}")

    # 3. Separación de registros
    df_buenos = df_unicos[mask_valid].copy()
    df_malos = df_unicos[~mask_valid].copy()
    
    # Registramos por qué fallaron (opcional: podrías detallar el error)
    if not df_malos.empty:
        registrar_rechazos(df_malos, nombre_archivo, "fallo_rango_o_nulo")
        logging.warning(f"🚨 {len(df_malos)} registros fuera de rango o con nulos en {nombre_archivo}")

    return df_buenos, pd.concat([duplicados, df_malos])