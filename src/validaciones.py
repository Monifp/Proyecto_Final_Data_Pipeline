import pandas as pd
import logging
import os
from datetime import datetime
from config import LOG_DIR


LOG_VALIDACION = os.path.join(LOG_DIR, "validaciones.log")

def validate_data(df, nombre_archivo):
    errores = 0
    logging.info(f"🔍 Validando: {nombre_archivo}")

    # NOT NULL para campos criticos
    criticos = ['id_venta', 'fecha', 'id_producto', 'cantidad', 'precio']
    for col in criticos:
        if col in df.columns and df[col].isnull().any():
            logging.error(f"❌ {nombre_archivo}: Nulos detectados en '{col}'")
            errores += 1

    # Validacion de tipos de archivos
    numericos = ['cantidad', 'precio', 'descuento']
    for col in numericos:
        if col in df.columns:
        
            if pd.to_numeric(df[col], errors='coerce').isnull().any():
                logging.error(f"❌ {nombre_archivo}: Datos no numéricos en '{col}'")
                errores += 1

    # control de rangos: fechas no futuras y Cantidad > 0
    if 'fecha' in df.columns:
        fechas = pd.to_datetime(df['fecha'], errors='coerce')
        if (fechas > datetime.now()).any():
            logging.error(f"❌ {nombre_archivo}: Se detectaron fechas futuras")
            errores += 1
            
    if 'cantidad' in df.columns:
        if (pd.to_numeric(df['cantidad'], errors='coerce') <= 0).any():
            logging.error(f"❌ {nombre_archivo}: Cantidades menores o iguales a cero")
            errores += 1

    # Retorno True/False solicitado
    if errores > 0:
        return False
    return True