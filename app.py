from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, avg
from pyspark.sql.types import TimestampType


spark = SparkSession.builder \
    .appName("NYC Yellow Taxi 2025 Analysis") \
    .getOrCreate()


df = spark.read.parquet("/app/data/*.parquet")  


df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType())) \
       .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType()))


start_date = "2025-01-01"
end_date = "2025-06-30"

df_clean = df.filter(
    (col("tpep_pickup_datetime") >= start_date) &
    (col("tpep_pickup_datetime") <= end_date) &
    (col("tpep_dropoff_datetime") >= start_date) &
    (col("tpep_dropoff_datetime") <= end_date) &
    (col("trip_distance") > 0) &
    (col("passenger_count") > 0)
)


df_clean = df_clean.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                   .withColumn("dropoff_hour", hour(col("tpep_dropoff_datetime")))


df_clean = df_clean.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "total_amount",
    "pickup_hour",
    "dropoff_hour"
)


zones = spark.read.csv("/app/taxi_zone_lookup.csv", header=True, inferSchema=True)


df_clean = df_clean.join(zones.select(col("LocationID").alias("PULocationID_zone"), col("Zone").alias("pickup_zone")),
                         df_clean.PULocationID == col("PULocationID_zone"), "left") \
                   .drop("PULocationID_zone")


df_clean = df_clean.join(zones.select(col("LocationID").alias("DOLocationID_zone"), col("Zone").alias("dropoff_zone")),
                         df_clean.DOLocationID == col("DOLocationID_zone"), "left") \
                   .drop("DOLocationID_zone")


hourly_counts = df_clean.groupBy("pickup_zone", "pickup_hour") \
                        .agg(count("*").alias("num_trips"))


pivot_df = hourly_counts.groupBy("pickup_zone") \
    .pivot("pickup_hour", list(range(24))) \
    .agg(avg("num_trips")) \
    .fillna(0)  


pivot_df.write.mode("overwrite").parquet("/app/data/yellow_taxi_hourly_avg.parquet")


pivot_df.show(10)

spark.stop()
