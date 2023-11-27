/* Create summary view of infrastructure objects by type 
   and apartments on current day*/
CREATE OR REPLACE VIEW INFRA_VIEW AS
SELECT ap.inx, d."name", d.population, d.area, d.density, d.metro, d.stops,
       ap."type", ap.subtype, ap.lat, ap.lon
FROM districts d
JOIN 
(SELECT ai.id + 10000 AS inx, ai.district_id, 'Квартира' AS "type", ai.rooms AS subtype, ai.lat, ai.lon
FROM apartment.apartment.apartments_info ai
WHERE ai.date = CURRENT_DATE
UNION ALL
SELECT hi.id + 20000 AS inx, hi.district_id, 'Больница' AS "type", hi."type" AS subtype, hi.lat, hi.lon
FROM apartment.apartment.hospitals_info hi 
UNION ALL
SELECT si.id + 30000 AS inx, si.district_id, 'Школа' AS "type", '' AS subtype, si.lat, si.lon
FROM apartment.apartment.schools_info si 
UNION ALL
SELECT ki.id + 10000 AS inx, ki.district_id, 'Детсад' AS "type", '' AS subtype, ki.lat, ki.lon
FROM apartment.apartment.kindergarten_info ki 
UNION ALL
SELECT mi.id + 10000 AS inx, mi.district_id, 'МФЦ' AS "type", '' AS subtype, mi.lat, mi.lon
FROM apartment.apartment.mfc_info mi) AS ap
ON d.id = ap.district_id
WHERE ap.lat != 0;