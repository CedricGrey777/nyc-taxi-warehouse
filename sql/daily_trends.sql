-- One row per observed pickup date across the published months.
-- Charges are recorded total_amount, not company net revenue.
SELECT pickup_date,
       sum(trip_record_count) AS trip_record_count,
       sum(recorded_charge_total) AS recorded_charge_total_usd,
       sum(distance_total_miles) / sum(trip_record_count) AS avg_trip_distance_miles
FROM mart_daily_pickup_zone
GROUP BY pickup_date
ORDER BY pickup_date;
