-- 1. DIMENSIÓN PRODUCTOS
CREATE OR REPLACE TABLE dim_productos AS 
SELECT 
    row_number() OVER () AS sk_producto,
    id_producto,
    nombre_producto,
    replace(precio_unitario, ',', '.')::DOUBLE AS precio_unitario
FROM (
    SELECT DISTINCT id_producto, nombre_producto, precio_unitario 
    FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/productos_limpio.csv', all_varchar=True)
);

-- 2. DIMENSIÓN CLIENTES
CREATE OR REPLACE TABLE dim_clientes AS 
SELECT 
    row_number() OVER () AS sk_cliente,
    id_cliente,
    nombre,
    apellido
FROM (
    SELECT DISTINCT id_cliente, nombre, apellido 
    FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/clientes_limpio.csv', all_varchar=True)
);

-- 3. DIMENSIÓN MÉTODOS DE PAGO
CREATE OR REPLACE TABLE dim_metodos_pago AS 
SELECT 
    row_number() OVER () AS sk_metodo_pago,
    metodo
FROM (
    SELECT DISTINCT metodo 
    FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/metodos_pago_limpio.csv', columns={'metodo': 'VARCHAR'})
);

-- 4. TABLA DE HECHOS (FACT TABLE)
CREATE OR REPLACE TABLE fct_ventas AS 
SELECT 
    v.id_venta,
    strptime(v.fecha::VARCHAR, '%d/%m/%Y')::DATE AS fecha_id, 
    c.sk_cliente,
    p.sk_producto,
    mp.sk_metodo_pago,
    v.cantidad::INT AS cantidad,
    v.estado,
    (v.cantidad::INT * p.precio_unitario) AS monto_total
FROM read_csv_auto('/Users/monyfpose/DataIan/Proyecto_Final/data/processed/ventas_limpio.csv', all_varchar=True) v
LEFT JOIN dim_productos p ON v.id_producto = p.id_producto
LEFT JOIN dim_clientes c ON v.id_cliente = c.id_cliente
LEFT JOIN dim_metodos_pago mp ON v.metodo_pago = mp.metodo;

-- 5. DIMENSIÓN CALENDARIO
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