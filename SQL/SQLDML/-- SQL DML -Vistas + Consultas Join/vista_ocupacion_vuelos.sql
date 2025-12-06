CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `andinovuelo`.`vista_ocupacion_vuelos` AS
    SELECT 
        `v`.`id` AS `vuelo_id`,
        `v`.`numero_vuelo` AS `numero_vuelo`,
        `v`.`fecha` AS `fecha`,
        `al`.`nombre` AS `aerolinea`,
        `v`.`capacidad_pasajeros` AS `capacidad_pasajeros`,
        COUNT(`t`.`id`) AS `pasajeros_registrados`,
        (`v`.`capacidad_pasajeros` - COUNT(`t`.`id`)) AS `asientos_disponibles`,
        ROUND(((COUNT(`t`.`id`) / `v`.`capacidad_pasajeros`) * 100),
                2) AS `porcentaje_ocupacion`,
        (CASE
            WHEN ((COUNT(`t`.`id`) / `v`.`capacidad_pasajeros`) >= 0.9) THEN 'LLENO'
            WHEN ((COUNT(`t`.`id`) / `v`.`capacidad_pasajeros`) >= 0.7) THEN 'ALTA'
            WHEN ((COUNT(`t`.`id`) / `v`.`capacidad_pasajeros`) >= 0.5) THEN 'MEDIA'
            ELSE 'BAJA'
        END) AS `categoria_ocupacion`
    FROM
        ((`andinovuelo`.`vuelo` `v`
        JOIN `andinovuelo`.`aerolinea` `al` ON ((`v`.`aerolinea_id` = `al`.`id`)))
        LEFT JOIN `andinovuelo`.`ticket_aereo` `t` ON ((`v`.`id` = `t`.`vuelo_id`)))
    GROUP BY `v`.`id` , `v`.`numero_vuelo` , `v`.`fecha` , `al`.`nombre` , `v`.`capacidad_pasajeros`