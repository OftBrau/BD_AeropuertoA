-- ============================================
-- SQL STORED PROCEDURES IN, OUT, INOUT, VARIABLES
-- ============================================

-- PROCEDIMIENTO 1: Parámetros IN - Buscar vuelos por aerolínea y fecha
DELIMITER $$
CREATE PROCEDURE sp_buscar_vuelos_aerolinea(
    IN p_aerolinea_id INT,
    IN p_fecha_inicio DATE,
    IN p_fecha_fin DATE
)
BEGIN
    SELECT 
        v.id,
        v.numero_vuelo,
        v.fecha,
        v.hora_salida_programada,
        ao.ciudad AS origen,
        ad.ciudad AS destino,
        v.resultado,
        v.capacidad_pasajeros
    FROM vuelo v
    INNER JOIN aeropuerto ao ON v.aeropuerto_origen_id = ao.id
    INNER JOIN aeropuerto ad ON v.aeropuerto_destino_id = ad.id
    WHERE v.aerolinea_id = p_aerolinea_id
      AND v.fecha BETWEEN p_fecha_inicio AND p_fecha_fin
    ORDER BY v.fecha, v.hora_salida_programada;
END$$
DELIMITER ;

-- Llamada: CALL sp_buscar_vuelos_aerolinea(1, '2024-01-01', '2024-12-31');


-- PROCEDIMIENTO 2: Parámetros OUT - Obtener estadísticas de un vuelo
DELIMITER $$
CREATE PROCEDURE sp_estadisticas_vuelo(
    IN p_vuelo_id INT,
    OUT p_total_pasajeros INT,
    OUT p_total_equipaje INT,
    OUT p_check_in_completados INT,
    OUT p_porcentaje_ocupacion DECIMAL(5,2)
)
BEGIN
    DECLARE v_capacidad INT;
    
    -- Obtener capacidad del vuelo
    SELECT capacidad_pasajeros INTO v_capacidad
    FROM vuelo
    WHERE id = p_vuelo_id;
    
    -- Contar pasajeros
    SELECT COUNT(*) INTO p_total_pasajeros
    FROM ticket_aereo
    WHERE vuelo_id = p_vuelo_id;
    
    -- Contar equipaje
    SELECT COUNT(*) INTO p_total_equipaje
    FROM equipaje
    WHERE vuelo_id = p_vuelo_id;
    
    -- Contar check-ins completados
    SELECT COUNT(*) INTO p_check_in_completados
    FROM ticket_aereo t
    INNER JOIN pasajero p ON t.pasajero_id = p.id
    WHERE t.vuelo_id = p_vuelo_id AND p.check_in = 1;
    
    -- Calcular porcentaje de ocupación
    IF v_capacidad > 0 THEN
        SET p_porcentaje_ocupacion = (p_total_pasajeros / v_capacidad) * 100;
    ELSE
        SET p_porcentaje_ocupacion = 0;
    END IF;
END$$
DELIMITER ;

-- Llamada:
-- CALL sp_estadisticas_vuelo(1, @pasajeros, @equipaje, @checkins, @ocupacion);
-- SELECT @pasajeros, @equipaje, @checkins, @ocupacion;


-- PROCEDIMIENTO 3: Parámetros INOUT - Actualizar y retornar contador
DELIMITER $$
CREATE PROCEDURE sp_procesar_embarque(
    IN p_ticket_id BIGINT,
    IN p_vuelo_id INT,
    IN p_puerta_id INT,
    INOUT p_contador_embarques INT
)
BEGIN
    DECLARE v_existe INT DEFAULT 0;
    
    -- Verificar si el ticket existe
    SELECT COUNT(*) INTO v_existe
    FROM ticket_aereo
    WHERE id = p_ticket_id AND vuelo_id = p_vuelo_id;
    
    IF v_existe > 0 THEN
        -- Insertar registro de embarque
        INSERT INTO embarque (vuelo_id, ticket_aereo_id, puerta_id, hora_embarque, estado)
        VALUES (p_vuelo_id, p_ticket_id, p_puerta_id, NOW(), 'OK');
        
        -- Incrementar contador
        SET p_contador_embarques = p_contador_embarques + 1;
        
        -- Registrar en log
        INSERT INTO log_cambios (quien, que, entidad_id, accion, detalles)
        VALUES ('SISTEMA', 'embarque', p_ticket_id, 'INSERT', 
                CONCAT('Embarque procesado. Total: ', p_contador_embarques));
    ELSE
        -- No hacer nada si el ticket no existe
        SET p_contador_embarques = p_contador_embarques;
    END IF;
END$$
DELIMITER ;

-- Llamada:
-- SET @contador = 0;
-- CALL sp_procesar_embarque(1, 1, 5, @contador);
-- SELECT @contador;


-- PROCEDIMIENTO 4: Variables locales y control de flujo
DELIMITER $$
CREATE PROCEDURE sp_clasificar_vuelo(
    IN p_vuelo_id INT,
    OUT p_clasificacion VARCHAR(50),
    OUT p_mensaje TEXT
)
BEGIN
    DECLARE v_ocupacion DECIMAL(5,2);
    DECLARE v_capacidad INT;
    DECLARE v_pasajeros INT;
    DECLARE v_estado VARCHAR(20);
    
    -- Obtener datos del vuelo
    SELECT 
        capacidad_pasajeros,
        resultado
    INTO v_capacidad, v_estado
    FROM vuelo
    WHERE id = p_vuelo_id;
    
    -- Contar pasajeros
    SELECT COUNT(*) INTO v_pasajeros
    FROM ticket_aereo
    WHERE vuelo_id = p_vuelo_id;
    
    -- Calcular ocupación
    IF v_capacidad > 0 THEN
        SET v_ocupacion = (v_pasajeros / v_capacidad) * 100;
    ELSE
        SET v_ocupacion = 0;
    END IF;
    
    -- Clasificar según ocupación
    IF v_ocupacion >= 95 THEN
        SET p_clasificacion = 'COMPLETO';
        SET p_mensaje = CONCAT('Vuelo lleno. Ocupación: ', ROUND(v_ocupacion, 2), '%');
    ELSEIF v_ocupacion >= 80 THEN
        SET p_clasificacion = 'ALTA_DEMANDA';
        SET p_mensaje = CONCAT('Alta demanda. Ocupación: ', ROUND(v_ocupacion, 2), '%');
    ELSEIF v_ocupacion >= 50 THEN
        SET p_clasificacion = 'MEDIA_DEMANDA';
        SET p_mensaje = CONCAT('Demanda media. Ocupación: ', ROUND(v_ocupacion, 2), '%');
    ELSEIF v_ocupacion > 0 THEN
        SET p_clasificacion = 'BAJA_DEMANDA';
        SET p_mensaje = CONCAT('Baja demanda. Ocupación: ', ROUND(v_ocupacion, 2), '%');
    ELSE
        SET p_clasificacion = 'SIN_RESERVAS';
        SET p_mensaje = 'No hay reservas para este vuelo';
    END IF;
END$$
DELIMITER ;

-- Llamada:
-- CALL sp_clasificar_vuelo(1, @clase, @msg);
-- SELECT @clase, @msg;


-- PROCEDIMIENTO 5: Cursor y loop - Generar reporte de pasajeros frecuentes
DELIMITER $$
CREATE PROCEDURE sp_pasajeros_frecuentes(
    IN p_minimo_vuelos INT
)
BEGIN
    DECLARE v_done INT DEFAULT FALSE;
    DECLARE v_pasajero_id INT;
    DECLARE v_nombre VARCHAR(80);
    DECLARE v_apellido VARCHAR(80);
    DECLARE v_total_vuelos INT;
    
    -- Cursor para recorrer pasajeros
    DECLARE cur_pasajeros CURSOR FOR
        SELECT 
            p.id,
            p.nombre,
            p.apellido,
            COUNT(t.id) AS total_vuelos
        FROM pasajero p
        INNER JOIN ticket_aereo t ON p.id = t.pasajero_id
        GROUP BY p.id, p.nombre, p.apellido
        HAVING COUNT(t.id) >= p_minimo_vuelos
        ORDER BY total_vuelos DESC;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
    
    -- Crear tabla temporal para resultados
    DROP TEMPORARY TABLE IF EXISTS tmp_pasajeros_frecuentes;
    CREATE TEMPORARY TABLE tmp_pasajeros_frecuentes (
        pasajero_id INT,
        nombre_completo VARCHAR(161),
        total_vuelos INT,
        categoria VARCHAR(20)
    );
    
    OPEN cur_pasajeros;
    
    read_loop: LOOP
        FETCH cur_pasajeros INTO v_pasajero_id, v_nombre, v_apellido, v_total_vuelos;
        
        IF v_done THEN
            LEAVE read_loop;
        END IF;
        
        -- Insertar en tabla temporal con categoría
        INSERT INTO tmp_pasajeros_frecuentes
        VALUES (
            v_pasajero_id,
            CONCAT(v_nombre, ' ', v_apellido),
            v_total_vuelos,
            CASE 
                WHEN v_total_vuelos >= 20 THEN 'PLATINUM'
                WHEN v_total_vuelos >= 10 THEN 'GOLD'
                WHEN v_total_vuelos >= 5 THEN 'SILVER'
                ELSE 'BRONZE'
            END
        );
    END LOOP;
    
    CLOSE cur_pasajeros;
    
    -- Mostrar resultados
    SELECT * FROM tmp_pasajeros_frecuentes;
END$$
DELIMITER ;

-- Llamada: CALL sp_pasajeros_frecuentes(3);


-- PROCEDIMIENTO 6: Transacciones y manejo de errores
DELIMITER $$
CREATE PROCEDURE sp_registrar_vuelo_completo(
    IN p_numero_vuelo VARCHAR(20),
    IN p_fecha DATE,
    IN p_hora_salida DATETIME,
    IN p_hora_llegada DATETIME,
    IN p_aeronave_id INT,
    IN p_aerolinea_id INT,
    IN p_origen_id INT,
    IN p_destino_id INT,
    OUT p_vuelo_id INT,
    OUT p_resultado VARCHAR(100)
)
BEGIN
    DECLARE v_capacidad INT;
    DECLARE v_existe INT;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SET p_vuelo_id = NULL;
        SET p_resultado = 'ERROR: No se pudo registrar el vuelo';
    END;
    
    START TRANSACTION;
    
    -- Verificar que no exista vuelo duplicado
    SELECT COUNT(*) INTO v_existe
    FROM vuelo
    WHERE numero_vuelo = p_numero_vuelo AND fecha = p_fecha;
    
    IF v_existe > 0 THEN
        SET p_vuelo_id = NULL;
        SET p_resultado = 'ERROR: Ya existe un vuelo con ese número en esa fecha';
        ROLLBACK;
    ELSE
        -- Obtener capacidad de la aeronave
        SELECT capacidad_pasajeros INTO v_capacidad
        FROM aeronave
        WHERE id = p_aeronave_id;
        
        -- Insertar vuelo
        INSERT INTO vuelo (
            numero_vuelo, fecha, hora_salida_programada, llegada_programada,
            aeronave_id, aerolinea_id, aeropuerto_origen_id, aeropuerto_destino_id,
            capacidad_pasajeros
        ) VALUES (
            p_numero_vuelo, p_fecha, p_hora_salida, p_hora_llegada,
            p_aeronave_id, p_aerolinea_id, p_origen_id, p_destino_id,
            v_capacidad
        );
        
        SET p_vuelo_id = LAST_INSERT_ID();
        
        -- Registrar en log
        INSERT INTO log_cambios (quien, que, entidad_id, accion, detalles)
        VALUES ('SISTEMA', 'vuelo', p_vuelo_id, 'INSERT', 
                CONCAT('Vuelo creado: ', p_numero_vuelo));
        
        SET p_resultado = 'OK: Vuelo registrado exitosamente';
        COMMIT;
    END IF;
END$$
DELIMITER ;

-- Llamada:
-- CALL sp_registrar_vuelo_completo('LA123', '2024-06-01', '2024-06-01 10:00:00', 
--      '2024-06-01 12:00:00', 1, 1, 1, 2, @vid, @res);
-- SELECT @vid, @res;


-- PROCEDIMIENTO 7: Variables y cálculos complejos
DELIMITER $$
CREATE PROCEDURE sp_analizar_rentabilidad_ruta(
    IN p_origen_id INT,
    IN p_destino_id INT,
    IN p_dias_atras INT,
    OUT p_total_vuelos INT,
    OUT p_promedio_ocupacion DECIMAL(5,2),
    OUT p_total_pasajeros INT,
    OUT p_rentabilidad VARCHAR(20)
)
BEGIN
    DECLARE v_capacidad_total INT DEFAULT 0;
    DECLARE v_pasajeros_total INT DEFAULT 0;
    
    -- Contar vuelos en la ruta
    SELECT COUNT(*) INTO p_total_vuelos
    FROM vuelo
    WHERE aeropuerto_origen_id = p_origen_id
      AND aeropuerto_destino_id = p_destino_id
      AND fecha >= CURDATE() - INTERVAL p_dias_atras DAY;
    
    -- Calcular capacidad total y pasajeros
    SELECT 
        SUM(v.capacidad_pasajeros),
        COUNT(t.id)
    INTO v_capacidad_total, v_pasajeros_total
    FROM vuelo v
    LEFT JOIN ticket_aereo t ON v.id = t.vuelo_id
    WHERE v.aeropuerto_origen_id = p_origen_id
      AND v.aeropuerto_destino_id = p_destino_id
      AND v.fecha >= CURDATE() - INTERVAL p_dias_atras DAY;
    
    SET p_total_pasajeros = v_pasajeros_total;
    
    -- Calcular ocupación promedio
    IF v_capacidad_total > 0 THEN
        SET p_promedio_ocupacion = (v_pasajeros_total / v_capacidad_total) * 100;
    ELSE
        SET p_promedio_ocupacion = 0;
    END IF;
    
    -- Determinar rentabilidad
    IF p_promedio_ocupacion >= 80 THEN
        SET p_rentabilidad = 'ALTA';
    ELSEIF p_promedio_ocupacion >= 60 THEN
        SET p_rentabilidad = 'MEDIA';
    ELSEIF p_promedio_ocupacion >= 40 THEN
        SET p_rentabilidad = 'BAJA';
    ELSE
        SET p_rentabilidad = 'NO_RENTABLE';
    END IF;
END$$
DELIMITER ;

-- Llamada:
-- CALL sp_analizar_rentabilidad_ruta(1, 2, 30, @vuelos, @ocupacion, @pasajeros, @rent);
-- SELECT @vuelos, @ocupacion, @pasajeros, @rent;