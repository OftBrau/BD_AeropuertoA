CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `andinovuelo`.`vista_embarques_detalle` AS
    SELECT 
        `e`.`id` AS `embarque_id`,
        `e`.`hora_embarque` AS `hora_embarque`,
        `e`.`estado` AS `estado_embarque`,
        `v`.`numero_vuelo` AS `numero_vuelo`,
        `v`.`fecha` AS `fecha_vuelo`,
        CONCAT(`p`.`nombre`, ' ', `p`.`apellido`) AS `pasajero`,
        `p`.`documento` AS `documento`,
        `t`.`asiento` AS `asiento`,
        `pu`.`identificador` AS `puerta`,
        `ter`.`nombre` AS `terminal`,
        `al`.`nombre` AS `aerolinea`
    FROM
        ((((((`andinovuelo`.`embarque` `e`
        JOIN `andinovuelo`.`vuelo` `v` ON ((`e`.`vuelo_id` = `v`.`id`)))
        JOIN `andinovuelo`.`ticket_aereo` `t` ON ((`e`.`ticket_aereo_id` = `t`.`id`)))
        JOIN `andinovuelo`.`pasajero` `p` ON ((`t`.`pasajero_id` = `p`.`id`)))
        JOIN `andinovuelo`.`puerta` `pu` ON ((`e`.`puerta_id` = `pu`.`id`)))
        JOIN `andinovuelo`.`terminal` `ter` ON ((`pu`.`terminal_id` = `ter`.`id`)))
        JOIN `andinovuelo`.`aerolinea` `al` ON ((`v`.`aerolinea_id` = `al`.`id`)))