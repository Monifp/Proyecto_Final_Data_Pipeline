================================================================
🚀 SISTEMA DE ANÁLISIS DE VENTAS - PIPELINE DE DATOS
================================================================

Este sistema automatiza la ingesta, limpieza, validación y reporte
de datos de ventas utilizando un modelo de datos estrella.

----------------------------------------------------------------
📂 1. ESTRUCTURA DE CARPETAS
----------------------------------------------------------------
📁 /data
  📁 /ingested      <- Archivos CSV originales (Raw Data).
    📁 /new_data    <- Espacio para ventas_nuevas.csv.
  📁 /processed     <- Archivos CSV normalizados (Clean Data).
  📁 /rejected      <- Registros que fallaron las validaciones.
  📁 /database      <- Base de datos DuckDB (Proyecto_final.db).
  📁 /reports       <- Reportes finales para Excel (BI).
📁 /scripts         <- Consultas SQL de transformación.
📁 /src             <- Código fuente en Python.
📁 /logs            <- Historial de ejecución (run_pipeline.log).

----------------------------------------------------------------
🏗️ 2. CONFIGURACIÓN INICIAL (SETUP)
----------------------------------------------------------------
Este paso se realiza una sola vez para crear la estructura base:

1. Colocar los archivos maestros (ventas, productos, clientes) 
   en la carpeta: data/ingested/
2. Ejecutar el pipeline principal:
   > python3 src/run_pipeline.py

✅ Resultado: Se limpian los archivos raw, se mueven a /processed
y se genera la base de datos inicial con su modelo estrella.

----------------------------------------------------------------
🔄 3. FLUJO DE ADICIÓN DE NUEVOS REGISTROS (INCREMENTAL)
----------------------------------------------------------------
Para agregar ventas nuevas al sistema sin afectar el histórico:

1. 📥 Carga: Colocar el archivo en:
   data/ingested/new_data/ventas_nuevas.csv

2. ⚙️ Proceso: Ejecutar el script incremental:
   > python3 src/ingesta_incremental.py

Este script normaliza columnas, elimina duplicados, valida fechas
y vincula los registros nuevos con la base de datos DuckDB.

----------------------------------------------------------------
📊 4. GENERACIÓN DE REPORTES DE NEGOCIO
----------------------------------------------------------------
Para obtener los resultados finales y visualizarlos en Excel:

1. 📈 Ejecutar el script de transformación:
   > python3 src/ejecutar_transformaciones.py

2. 📁 Buscar los archivos en data/reports/:
   - VENTAS_POR_CATEGORIA.csv
   - VENTAS_POR_CLIENTE.csv

----------------------------------------------------------------
⚠️ 5. REGLAS DE ORO PARA EL USUARIO
----------------------------------------------------------------
📍 Los nombres de columnas se normalizan a minúsculas y sin tildes.
📍 El separador de reportes es ";" con decimales "," (Formato Excel).
📍 Validaciones: No se aceptan ventas anteriores a 1980 o futuras.
📍 Auditoría: Revisar logs/run_pipeline.log ante cualquier error.

=======================================================================
💻 Desarrollado con Python 3.12 & DuckDB - Asistido por Gemini Pro 
                       Human led - AI Powered 
=======================================================================