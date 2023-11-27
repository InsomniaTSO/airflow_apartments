CREATE MATERIALIZED VIEW mv_city_districts AS
WITH 
spb AS
(SELECT * 
FROM ru_mln_city_districts rmcd 
WHERE region = 'Санкт-Петербург')
SELECT d.id, d."name", d.population,
       d.area, d.density, d.metro, d.stops,
       spb.coords_type, spb.coords
FROM spb
JOIN districts d ON d."name" = spb.name;