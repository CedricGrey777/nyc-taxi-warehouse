-- Top 10 pickup zones by accepted source-record count; ties use the zone key.
SELECT m.pu_zone_key, z.borough, z.zone,
       sum(m.trip_record_count) AS trip_record_count,
       sum(m.recorded_charge_total) AS recorded_charge_total_usd,
       sum(m.distance_total_miles) / sum(m.trip_record_count) AS avg_trip_distance_miles
FROM mart_daily_pickup_zone m
JOIN dim_zone z ON m.pu_zone_key = z.zone_key
GROUP BY m.pu_zone_key, z.borough, z.zone
ORDER BY trip_record_count DESC, m.pu_zone_key
LIMIT 10;
