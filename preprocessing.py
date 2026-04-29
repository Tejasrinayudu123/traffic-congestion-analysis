from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, dayofweek, month, when

spark = SparkSession.builder.appName("TrafficPreprocessing").getOrCreate()

# Read raw data
df = spark.read.parquet("s3://smart-traffic-analysis/raw/yellow_tripdata_2023-01.parquet")
zone_df = spark.read.csv("s3://smart-traffic-analysis/lookup/taxi_zone_lookup.csv", header=True, inferSchema=True)

# Select required columns
trip_df = df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount"
)

# Drop nulls
trip_df = trip_df.dropna(subset=[
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount"
])

# Compute trip duration
trip_df = trip_df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
)

# Filter invalid trips
trip_df = trip_df.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("trip_duration_minutes") > 0) &
    (col("trip_duration_minutes") <= 180) &
    (col("passenger_count") >= 0)
)

# Feature engineering
trip_df = trip_df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                 .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime"))) \
                 .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

trip_df = trip_df.withColumn(
    "average_speed_mph",
    col("trip_distance") / (col("trip_duration_minutes") / 60)
)

trip_df = trip_df.withColumn(
    "congestion_flag",
    when((col("average_speed_mph") < 12) & (col("trip_duration_minutes") > 20), 1).otherwise(0)
)

# Join pickup zones
pickup_zone_df = zone_df.select(
    col("LocationID").alias("PULocationID"),
    col("Borough").alias("PU_Borough"),
    col("Zone").alias("PU_Zone")
)
trip_df = trip_df.join(pickup_zone_df, on="PULocationID", how="left")

# Join dropoff zones
dropoff_zone_df = zone_df.select(
    col("LocationID").alias("DOLocationID"),
    col("Borough").alias("DO_Borough"),
    col("Zone").alias("DO_Zone")
)
trip_df = trip_df.join(dropoff_zone_df, on="DOLocationID", how="left")

# Save cleaned data
trip_df.write.mode("overwrite").parquet("s3://smart-traffic-analysis/processed/cleaned_yellow_tripdata_2023_01.parquet")

spark.stop()