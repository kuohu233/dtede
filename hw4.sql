# Q3 
SELECT count(*) FROM `root-range-382301.trips_data_all.fct_monthly_zone_revenue`;

# Q4
SELECT zone, SUM(revenue_monthly_total_amount) as total_rev 
FROM `root-range-382301.trips_data_all.fct_monthly_zone_revenue`
WHERE service_type = 'Green' AND year = 2020
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

# Q5
SELECT service_type, year, month, sum(total_monthly_trips) as total_trips
FROM `root-range-382301.trips_data_all.fct_monthly_zone_revenue`
WHERE service_type = 'Green' AND year = 2019 AND month = 10
GROUP BY 1, 2, 3

# Q6
{{ config(materialized='view') }}

with tripregs as (
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata_2019') }}
  where dispatching_base_num is not null 
)
select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
    sr_flag,
    affiliated_base_number
from tripregs
where rn = 1
