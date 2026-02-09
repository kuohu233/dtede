# Create schema and tables
CREATE SCHEMA IF NOT EXISTS `root-range-382301.trips_data_all`;

CREATE OR REPLACE EXTERNAL TABLE `root-range-382301.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-2026-youyuu/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `root-range-382301.trips_data_all.yellow_tripdata_2024` AS
SELECT * FROM `root-range-382301.trips_data_all.external_yellow_tripdata`;

# Question 1
SELECT COUNT(*) FROM `root-range-382301.trips_data_all.yellow_tripdata_2024`;

# Question 2
SELECT COUNT(DISTINCT(PULocationID)) FROM `root-range-382301.trips_data_all.external_yellow_tripdata`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `root-range-382301.trips_data_all.yellow_tripdata_2024`;

# Question 4
SELECT COUNT(*) 
FROM `root-range-382301.trips_data_all.yellow_tripdata_2024`
WHERE fare_amount = 0;

# Question 5
CREATE OR REPLACE TABLE `root-range-382301.trips_data_all.yellow_tripdata_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `root-range-382301.trips_data_all.external_yellow_tripdata`;

# Question 6
SELECT DISTINCT(VendorID)
FROM `root-range-382301.trips_data_all.yellow_tripdata_2024`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID)
FROM `root-range-382301.trips_data_all.yellow_tripdata_2024_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

# Question 9
SELECT table_name 
FROM `root-range-382301.trips_data_all.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE'; -- BASE TABLE 指的就是原生表

SELECT count(*) FROM `root-range-382301.trips_data_all.yellow_tripdata_2024`;
