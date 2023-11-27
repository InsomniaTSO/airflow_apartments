/* Create summary view of infrastructure objects count by districts*/
CREATE OR REPLACE VIEW INFRA_COUNT_VIEW AS
WITH 
green_count AS (
SELECT oi.district_id, COUNT(oi.id) AS green_count
FROM objects_info oi 
WHERE oi.type_id IN (12, 21)
GROUP BY oi.district_id),
road_count AS (
SELECT oi.district_id, COUNT(oi.id) AS road_count
FROM objects_info oi 
WHERE oi.type_id = 5
GROUP BY oi.district_id),
hosp_count AS (
SELECT hi.district_id, COUNT(hi.id) AS hosp_count
FROM hospitals_info hi
GROUP BY hi.district_id),
school_count AS (
SELECT si.district_id, COUNT(si.id) AS school_count
FROM schools_info si
GROUP BY si.district_id),
kind_count AS (
SELECT ki.district_id, COUNT(ki.id) AS kind_count
FROM kindergarten_info ki 
GROUP BY ki.district_id),
sport_count AS (
SELECT si2.district_id, COUNT(si2.id) AS sport_count
FROM sportgrounds_info si2
GROUP BY si2.district_id)
SELECT d."name", gc.green_count, rc.road_count, 
       hc.hosp_count, cc.school_count, kc.kind_count,
       sc.sport_count
FROM green_count gc
JOIN road_count rc ON rc.district_id = gc.district_id
JOIN hosp_count hc ON rc.district_id = hc.district_id
JOIN school_count cc ON rc.district_id = cc.district_id
JOIN kind_count kc ON rc.district_id = kc.district_id
JOIN districts d ON d.id = rc.district_id
JOIN sport_count sc ON rc.district_id = sc.district_id;