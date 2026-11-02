-- Average distance is weighted by records, never an average of group averages.
-- observed_days counts dates with accepted records, not all calendar dates.
SELECT CASE WHEN d.is_weekend THEN 'weekend' ELSE 'weekday' END AS day_type,
       count(DISTINCT m.pickup_date) AS observed_days,
       sum(m.trip_record_count) AS trip_record_count,
       sum(m.recorded_charge_total) AS recorded_charge_total_usd,
       sum(m.distance_total_miles) / sum(m.trip_record_count) AS avg_trip_distance_miles,
       sum(m.trip_record_count) * 1.0 / count(DISTINCT m.pickup_date) AS avg_daily_record_count
FROM mart_daily_pickup_zone m
JOIN dim_date d ON m.pickup_date = d.calendar_date
GROUP BY d.is_weekend
ORDER BY d.is_weekend;
