import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize Session (Fixed method name)
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('homework') \
    .getOrCreate()

print(f"Spark Version: {spark.version}")

# 2. Load and Repartition
df = spark.read.parquet('yellow_tripdata_2025-11.parquet')

# This writes the 4 files to the folder
df.repartition(4).write.mode("overwrite").parquet('data/pq/yellow/2025/11/')

# 3. Question 3: Trips on Nov 15th (Added print)
count_nov_15 = df.withColumn('pickup_date', F.to_date(df.tpep_pickup_datetime)) \
    .filter(F.col('pickup_date') == '2025-11-15') \
    .count()
print(f"Trips on November 15th: {count_nov_15}")

# 4. Question 4: Longest Trip (Hours)
df.withColumn('duration_hrs', 
    (F.unix_timestamp(df.tpep_dropoff_datetime) - F.unix_timestamp(df.tpep_pickup_datetime)) / 3600) \
    .select(F.max('duration_hrs')) \
    .show()

# 5. Question 6: Least frequent pickup location
df_zones = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')

# Join using the common column name to avoid duplicates
df_joined = df.join(df_zones, df.PULocationID == df_zones.LocationID)

df_joined.groupBy('Zone') \
    .count() \
    .orderBy('count', ascending=True) \
    .show(5) # Show top 5 to see the absolute least
