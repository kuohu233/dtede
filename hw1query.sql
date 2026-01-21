--- Q3 
SELECT COUNT(*) 
FROM green_taxi_data 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;

--- Q4
SELECT 
    CAST(lpep_pickup_datetime AS DATE) AS pickup_day, 
    MAX(trip_distance) AS max_dist
FROM green_taxi_data
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY max_dist DESC
LIMIT 1;

--- Q5
SELECT 
    z."Zone", 
    SUM(t.total_amount) AS total_amount_sum
FROM green_taxi_data t
JOIN taxi_zones z 
    ON t."PULocationID" = z."LocationID"
WHERE CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;

--- Q6
SELECT 
    zd."Zone" AS dropoff_zone, 
    MAX(t.tip_amount) AS max_tip
FROM green_taxi_data t
JOIN taxi_zones zp 
    ON t."PULocationID" = zp."LocationID"
JOIN taxi_zones zd 
    ON t."DOLocationID" = zd."LocationID"
WHERE 
    zp."Zone" = 'East Harlem North' 
    AND t.lpep_pickup_datetime >= '2025-11-01' 
    AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY zd."Zone"
ORDER BY max_tip DESC
LIMIT 1;