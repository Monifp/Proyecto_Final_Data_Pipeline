 -- 1. TABLA ENRIQUECIDA (Hechos + Dimensiones)
CREATE OR REPLACE TABLE fct_ventas_enriquecida AS
SELECT 
    v.id_venta,
    v.fecha_id,
    v.sk_cliente,
    v.sk_producto,
    v.cantidad,
    p.precio_unitario,
    p.categoria,  -- <--- Ahora esta columna ya no falla!
    CAST((v.cantidad * p.precio_unitario) AS DECIMAL(18,2)) AS ingreso_total,
    CAST(v.fecha_id AS DATE) AS fecha_dt
FROM fct_ventas v
JOIN dim_productos p ON v.sk_producto = p.sk_producto;

-- 2. VISTA: VENTAS POR CATEGORÍA (La que te faltaba)
CREATE OR REPLACE VIEW reporte_ventas_categoria AS
SELECT 
    categoria,
    COUNT(id_venta) AS total_operaciones,
    SUM(ingreso_total) AS facturacion_total
FROM fct_ventas_enriquecida
GROUP BY categoria
ORDER BY facturacion_total DESC;

-- 3. VISTA: VENTAS POR CLIENTE
CREATE OR REPLACE VIEW reporte_ventas_cliente AS
SELECT 
    c.nombre,  -- Ajusta si se llama 'nombre_cliente' o 'nombre'
    COUNT(f.id_venta) AS total_transacciones,
    SUM(f.ingreso_total) AS facturacion_total
FROM fct_ventas_enriquecida f
JOIN dim_clientes c ON f.sk_cliente = c.sk_cliente
GROUP BY ALL
ORDER BY facturacion_total DESC;