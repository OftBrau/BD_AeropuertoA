CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `andinovuelo`.`vista_tickets_pasajeros` AS
    SELECT 
        `t`.`id` AS `ticket_id`,
        `t`.`pnr` AS `pnr`,
        `t`.`asiento` AS `asiento`,
        `t`.`estado_ticket` AS `estado_ticket`,
        `t`.`fecha_emision` AS `fecha_emision`,
        CONCAT(`p`.`nombre`, ' ', `p`.`apellido`) AS `nombre_pasajero`,
        `p`.`documento` AS `documento`,
        `p`.`email` AS `email`,
        `p`.`telefono` AS `telefono`,
        `p`.`check_in` AS `check_in`,
        `v`.`numero_vuelo` AS `numero_vuelo`,
        `v`.`fecha` AS `fecha_vuelo`,
        `v`.`hora_salida_programada` AS `hora_salida_programada`,
        `al`.`nombre` AS `aerolinea`,
        `ao`.`codigo_iata` AS `origen`,
        `ad`.`codigo_iata` AS `destino`
    FROM
        (((((`andinovuelo`.`ticket_aereo` `t`
        JOIN `andinovuelo`.`pasajero` `p` ON ((`t`.`pasajero_id` = `p`.`id`)))
        JOIN `andinovuelo`.`vuelo` `v` ON ((`t`.`vuelo_id` = `v`.`id`)))
        JOIN `andinovuelo`.`aerolinea` `al` ON ((`v`.`aerolinea_id` = `al`.`id`)))
        JOIN `andinovuelo`.`aeropuerto` `ao` ON ((`v`.`aeropuerto_origen_id` = `ao`.`id`)))
        JOIN `andinovuelo`.`aeropuerto` `ad` ON ((`v`.`aeropuerto_destino_id` = `ad`.`id`)))