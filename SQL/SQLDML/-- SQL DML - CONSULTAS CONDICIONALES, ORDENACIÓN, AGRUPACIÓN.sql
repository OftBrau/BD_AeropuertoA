-- ============================================
-- SQL DML - CONSULTAS CONDICIONALES, ORDENACIÓN, AGRUPACIÓN
-- ============================================

-- 1. Consulta con WHERE y ORDER BY
-- Listar todos los vuelos de una aerolínea específica ordenados por fecha
SELECT 
    v.numero_vuelo,
    v.fecha,
    v.hora_salida_programada,
    ao.nombre AS origen,
    ad.nombre AS destino,
    v.resultado
FROM vuelo v
INNER JOIN aeropuerto ao ON v.aeropuerto_origen_id = ao.id
INNER JOIN aeropuerto ad ON v.aeropuerto_destino_id = ad.id
WHERE v.aerolinea_id = 1
  AND v.fecha >= '2024-01-01'
ORDER BY v.fecha DESC, v.hora_salida_programada ASC;

-- 2. Consulta con GROUP BY y COUNT
-- Contar cantidad de vuelos por aerolínea
SELECT 
    a.nombre AS aerolinea,
    a.codigo_iata,
    COUNT(v.id) AS total_vuelos,
    COUNT(CASE WHEN v.resultado = 'COMPLETADO' THEN 1 END) AS vuelos_completados
FROM aerolinea a
LEFT JOIN vuelo v ON a.id = v.aerolinea_id
GROUP BY a.id, a.nombre, a.codigo_iata
ORDER BY total_vuelos DESC;

-- 3. Consulta con GROUP BY y SUM/AVG
-- Calcular ocupación promedio por vuelo
SELECT 
    v.numero_vuelo,
    v.fecha,
    v.capacidad_pasajeros,
    COUNT(t.id) AS pasajeros_registrados,
    ROUND((COUNT(t.id) / v.capacidad_pasajeros) * 100, 2) AS porcentaje_ocupacion
FROM vuelo v
LEFT JOIN ticket_aereo t ON v.id = t.vuelo_id
WHERE v.fecha >= CURDATE() - INTERVAL 30 DAY
GROUP BY v.id, v.numero_vuelo, v.fecha, v.capacidad_pasajeros
HAVING COUNT(t.id) > 0
ORDER BY porcentaje_ocupacion DESC;

-- 4. Consulta con HAVING y múltiples condiciones
-- Aeropuertos con más de 10 vuelos como origen en el último mes
SELECT 
    ap.codigo_iata,
    ap.nombre,
    ap.ciudad,
    ap.pais,
    COUNT(v.id) AS total_vuelos_origen
FROM aeropuerto ap
INNER JOIN vuelo v ON ap.id = v.aeropuerto_origen_id
WHERE v.fecha >= CURDATE() - INTERVAL 30 DAY
GROUP BY ap.id, ap.codigo_iata, ap.nombre, ap.ciudad, ap.pais
HAVING COUNT(v.id) > 10
ORDER BY total_vuelos_origen DESC;

-- 5. Consulta con múltiples JOINs y agregaciones
-- Estadísticas de pasajeros por aerolínea
SELECT 
    al.nombre AS aerolinea,
    COUNT(DISTINCT v.id) AS total_vuelos,
    COUNT(t.id) AS total_tickets,
    COUNT(DISTINCT p.id) AS pasajeros_unicos,
    COUNT(e.id) AS total_equipajes
FROM aerolinea al
INNER JOIN vuelo v ON al.id = v.aerolinea_id
LEFT JOIN ticket_aereo t ON v.id = t.vuelo_id
LEFT JOIN pasajero p ON t.pasajero_id = p.id
LEFT JOIN equipaje e ON t.id = e.ticket_aereo_id
WHERE v.fecha >= '2024-01-01'
GROUP BY al.id, al.nombre
ORDER BY total_tickets DESC;

-- 6. Consulta con subconsulta y WHERE
-- Pasajeros con más de 3 vuelos registrados
SELECT 
    p.id,
    CONCAT(p.nombre, ' ', p.apellido) AS nombre_completo,
    p.email,
    COUNT(t.id) AS total_vuelos,
    MAX(t.fecha_emision) AS ultimo_vuelo
FROM pasajero p
INNER JOIN ticket_aereo t ON p.id = t.pasajero_id
GROUP BY p.id, p.nombre, p.apellido, p.email
HAVING COUNT(t.id) > 3
ORDER BY total_vuelos DESC;

-- 7. Consulta con CASE y agrupación
-- Clasificar vuelos por estado y contar
SELECT 
    CASE 
        WHEN v.resultado = 'COMPLETADO' THEN 'Completado'
        WHEN v.resultado = 'CANCELADO' THEN 'Cancelado'
        WHEN v.resultado = 'EN_VUELO' THEN 'En Vuelo'
        WHEN v.resultado IS NULL THEN 'Programado'
        ELSE 'Otro'
    END AS estado_vuelo,
    COUNT(*) AS cantidad,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vuelo), 2) AS porcentaje
FROM vuelo v
GROUP BY estado_vuelo
ORDER BY cantidad DESC;

-- 8. Consulta con DATE functions y filtros
-- Vuelos por mes del año actual
SELECT 
    MONTH(v.fecha) AS mes,
    MONTHNAME(v.fecha) AS nombre_mes,
    COUNT(*) AS total_vuelos,
    COUNT(DISTINCT v.aerolinea_id) AS aerolineas_operando,
    SUM(v.capacidad_pasajeros) AS capacidad_total
FROM vuelo v
WHERE YEAR(v.fecha) = YEAR(CURDATE())
GROUP BY MONTH(v.fecha), MONTHNAME(v.fecha)
ORDER BY mes;

-- 9. Consulta con GROUP BY en múltiples niveles
-- Equipaje por estado y por vuelo
SELECT 
    v.numero_vuelo,
    v.fecha,
    e.estado,
    COUNT(e.id) AS cantidad_equipaje
FROM vuelo v
LEFT JOIN equipaje e ON v.id = e.vuelo_id
GROUP BY v.id, v.numero_vuelo, v.fecha, e.estado
ORDER BY v.fecha DESC, cantidad_equipaje DESC;

-- 10. Consulta compleja con múltiples agregaciones
-- Reporte completo de operaciones por aeropuerto
SELECT 
    ap.codigo_iata,
    ap.nombre AS aeropuerto,
    ap.ciudad,
    COUNT(DISTINCT vo.id) AS vuelos_salida,
    COUNT(DISTINCT vd.id) AS vuelos_llegada,
    COUNT(DISTINCT vo.id) + COUNT(DISTINCT vd.id) AS total_operaciones
FROM aeropuerto ap
LEFT JOIN vuelo vo ON ap.id = vo.aeropuerto_origen_id
LEFT JOIN vuelo vd ON ap.id = vd.aeropuerto_destino_id
GROUP BY ap.id, ap.codigo_iata, ap.nombre, ap.ciudad
HAVING total_operaciones > 0
ORDER BY total_operaciones DESC
LIMIT 10;