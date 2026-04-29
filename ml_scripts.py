from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("TrafficML").getOrCreate()

clean_df = spark.read.parquet("s3://smart-traffic-analysis/processed/cleaned_yellow_tripdata_2023_01.parquet")

ml_df = clean_df.select(
    "trip_duration_minutes",
    "trip_distance",
    "passenger_count",
    "pickup_hour",
    "pickup_dayofweek",
    "PULocationID",
    "DOLocationID"
).dropna()

ml_df = ml_df.filter(
    (col("trip_duration_minutes") >= 1) &
    (col("trip_duration_minutes") <= 120) &
    (col("trip_distance") > 0)
)

# Sample for Learner Lab efficiency
ml_df = ml_df.sample(False, 0.01, seed=42)

assembler = VectorAssembler(
    inputCols=[
        "trip_distance",
        "passenger_count",
        "pickup_hour",
        "pickup_dayofweek",
        "PULocationID",
        "DOLocationID"
    ],
    outputCol="features"
)

ml_df = assembler.transform(ml_df)

train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)

# Baseline model
lr = LinearRegression(featuresCol="features", labelCol="trip_duration_minutes")
lr_model = lr.fit(train_df)
lr_predictions = lr_model.transform(test_df)

# Final model
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="trip_duration_minutes",
    numTrees=3,
    maxDepth=4
)
rf_model = rf.fit(train_df)
rf_predictions = rf_model.transform(test_df)

# Evaluators
rmse_eval = RegressionEvaluator(labelCol="trip_duration_minutes", predictionCol="prediction", metricName="rmse")
mae_eval = RegressionEvaluator(labelCol="trip_duration_minutes", predictionCol="prediction", metricName="mae")
r2_eval = RegressionEvaluator(labelCol="trip_duration_minutes", predictionCol="prediction", metricName="r2")

# Metrics
lr_rmse = rmse_eval.evaluate(lr_predictions)
lr_mae = mae_eval.evaluate(lr_predictions)
lr_r2 = r2_eval.evaluate(lr_predictions)

rf_rmse = rmse_eval.evaluate(rf_predictions)
rf_mae = mae_eval.evaluate(rf_predictions)
rf_r2 = r2_eval.evaluate(rf_predictions)

# Save predictions
rf_predictions.select(
    "trip_duration_minutes",
    "prediction",
    "trip_distance",
    "pickup_hour",
    "PULocationID",
    "DOLocationID"
).write.mode("overwrite").csv("s3://smart-traffic-analysis/ml/ml_predictions/", header=True)

# Save metrics as a small dataframe
metrics_data = [
    ("Linear Regression", lr_rmse, lr_mae, lr_r2),
    ("Random Forest", rf_rmse, rf_mae, rf_r2)
]

metrics_df = spark.createDataFrame(metrics_data, ["model", "rmse", "mae", "r2"])
metrics_df.write.mode("overwrite").csv("s3://smart-traffic-analysis/ml/ml_metrics/", header=True)

spark.stop()