-- 1. DIMENSIÓN PRODUCTOS (Dinámica)
CREATE OR REPLACE TABLE dim_productos AS 
SELECT 
    row_number() OVER () AS sk_producto,
    -- Tomamos todo el resto de las columnas (incluye categoria, marca, etc.)
    * EXCLUDE (precio_unitario),
    -- Transformamos solo el precio
    replace(precio_unitario, ',', '.')::DOUBLE AS precio_unitario
FROM (
    SELECT DISTINCT * FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/productos_limpio.csv', all_varchar=True)
);

-- 2. DIMENSIÓN CLIENTES (Dinámica)
CREATE OR REPLACE TABLE dim_clientes AS 
SELECT 
    row_number() OVER () AS sk_cliente,
    *
FROM (
    SELECT DISTINCT * FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/clientes_limpio.csv', all_varchar=True)
);

-- 3. DIMENSIÓN MÉTODOS DE PAGO (Dinámica)
CREATE OR REPLACE TABLE dim_metodos_pago AS 
SELECT 
    row_number() OVER () AS sk_metodo_pago,
    *
FROM (
    SELECT DISTINCT * FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/metodos_pago_limpio.csv', all_varchar=True)
);

-- 4. TABLA DE HECHOS (FACT TABLE)
CREATE OR REPLACE TABLE fct_ventas AS 
SELECT 
    v.* EXCLUDE (fecha, cantidad, metodo_pago, id_producto, id_cliente), -- Excluimos originales para reemplazarlas
    strptime(v.fecha::VARCHAR, '%d/%m/%Y')::DATE AS fecha_id, 
    c.sk_cliente,
    p.sk_producto,
    mp.sk_metodo_pago,
    v.cantidad::INT AS cantidad,
    -- El monto total se calcula dinámicamente con el precio de la dimensión
    (v.cantidad::INT * p.precio_unitario) AS monto_total
FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/ventas_limpio.csv', all_varchar=True) v
LEFT JOIN dim_productos p ON v.id_producto = p.id_producto
LEFT JOIN dim_clientes c ON v.id_cliente = c.id_cliente
LEFT JOIN dim_metodos_pago mp ON v.metodo_pago = mp.metodo;

-- 5. DIMENSIÓN CALENDARIO (Se mantiene igual, es independiente de los CSV)
CREATE OR REPLACE TABLE dim_calendario AS
SELECT
    column0 AS fecha_id,
    year(column0) AS anio,
    month(column0) AS mes,
    monthname(column0) AS nombre_mes,
    dayofweek(column0) AS dia_semana,
    quarter(column0) AS trimestre
FROM (
    SELECT range AS column0 
    FROM range(DATE '2023-01-01', DATE '2027-12-31', INTERVAL '1 day')
);