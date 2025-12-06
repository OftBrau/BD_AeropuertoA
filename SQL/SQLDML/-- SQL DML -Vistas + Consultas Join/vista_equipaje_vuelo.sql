CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `andinovuelo`.`vista_equipaje_vuelo` AS
    SELECT 
        `v`.`numero_vuelo` AS `numero_vuelo`,
        `v`.`fecha` AS `fecha`,
        COUNT(`e`.`id`) AS `total_equipaje`,
        COUNT((CASE
            WHEN (`e`.`estado` = 'REGISTRADO') THEN 1
        END)) AS `equipaje_registrado`,
        COUNT((CASE
            WHEN (`e`.`estado` = 'EN_TRANSITO') THEN 1
        END)) AS `equipaje_transito`,
        COUNT((CASE
            WHEN (`e`.`estado` = 'ENTREGADO') THEN 1
        END)) AS `equipaje_entregado`,
        COUNT((CASE
            WHEN (`e`.`estado` = 'PERDIDO') THEN 1
        END)) AS `equipaje_perdido`
    FROM
        (`andinovuelo`.`vuelo` `v`
        LEFT JOIN `andinovuelo`.`equipaje` `e` ON ((`v`.`id` = `e`.`vuelo_id`)))
    GROUP BY `v`.`id` , `v`.`numero_vuelo` , `v`.`fecha`