/* Create materialized view of average price by date, district, rooms */
CREATE MATERIALIZED VIEW mv_apartments_for_days AS
SELECT ai."date", ai.district_id, rooms, count(id) AS apart_count, 
       avg(price) AS apart_avg_price
FROM apartments_info ai
GROUP BY ai."date", ai.district_id, rooms;