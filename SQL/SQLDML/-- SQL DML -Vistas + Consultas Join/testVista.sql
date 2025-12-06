-- Ver vuelos completos
SELECT * FROM vista_vuelos_completos LIMIT 10;

-- Ver tickets con pasajeros
SELECT * FROM vista_tickets_pasajeros LIMIT 10;

-- Ver ocupación de vuelos
SELECT * FROM vista_ocupacion_vuelos ORDER BY porcentaje_ocupacion DESC LIMIT 10;

-- Ver embarques
SELECT * FROM vista_embarques_detalle LIMIT 10;

-- Ver equipaje por vuelo
SELECT * FROM vista_equipaje_vuelo WHERE total_equipaje > 0 LIMIT 10;