from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when

spark = SparkSession.builder.appName("TrafficAnalytics").getOrCreate()

clean_df = spark.read.parquet("s3://smart-traffic-analysis/processed/cleaned_yellow_tripdata_2023_01.parquet")

# Hourly trips
hourly_trips = clean_df.groupBy("pickup_hour").count().orderBy("pickup_hour")
hourly_trips.write.mode("overwrite").csv("s3://smart-traffic-analysis/analytics/hourly_trips/", header=True)

# Top pickup zones
top_pickup_zones = clean_df.groupBy("PU_Borough", "PU_Zone").count().orderBy(col("count").desc())
top_pickup_zones.write.mode("overwrite").csv("s3://smart-traffic-analysis/analytics/top_pickup_zones/", header=True)

# Average duration by hour
avg_duration_by_hour = clean_df.groupBy("pickup_hour").agg(
    avg("trip_duration_minutes").alias("avg_trip_duration")
).orderBy("pickup_hour")
avg_duration_by_hour.write.mode("overwrite").csv("s3://smart-traffic-analysis/analytics/avg_duration_by_hour/", header=True)

# Weekday vs weekend
clean_df = clean_df.withColumn(
    "day_type",
    when(col("pickup_dayofweek").isin(1, 7), "Weekend").otherwise("Weekday")
)

daytype_stats = clean_df.groupBy("day_type").agg(
    avg("trip_duration_minutes").alias("avg_duration"),
    avg("average_speed_mph").alias("avg_speed")
)
daytype_stats.write.mode("overwrite").csv("s3://smart-traffic-analysis/analytics/daytype_stats/", header=True)

# Congestion hotspots
hotspots = clean_df.groupBy("PU_Borough", "PU_Zone").agg(
    avg("trip_duration_minutes").alias("avg_duration"),
    avg("average_speed_mph").alias("avg_speed"),
    avg("congestion_flag").alias("congestion_ratio")
).orderBy(col("congestion_ratio").desc(), col("avg_duration").desc())

hotspots.write.mode("overwrite").csv("s3://smart-traffic-analysis/analytics/congestion_hotspots/", header=True)

spark.stop()