# Q1
SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) 
FROM ny_taxi_data.taxi_rides;

# Q2
SELECT 
    (COUNT(CASE WHEN payment_type = 1 THEN 1 END) * 100.0 / COUNT(*)) AS credit_card_percentage
FROM ny_taxi_data.taxi_rides;

# Q3 
SELECT SUM(tip_amount) FROM ny_taxi_data.taxi_rides;
