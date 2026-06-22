from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, count, sum, window
import os

os.makedirs("data/input_stream", exist_ok=True)

spark = SparkSession.builder \
    .appName("LAB11_StructuredStreaming") \
    .master("local[*]") \
    .getOrCreate()

print(f"Wersja Apache Spark: {spark.version}")

schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

df_stream = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("data/input_stream")

df_cleaned = df_stream.withColumn("event_time", to_timestamp(col("event_time")))
print(f"Czy DataFrame jest strumieniowy? {df_cleaned.isStreaming}")
df_cleaned.printSchema()

summary_query = df_cleaned.filter(col("status") == "paid") \
    .groupBy("category") \
    .agg(count("*").alias("events_count"), sum("amount").alias("total_amount"))

watermarked_df = df_cleaned.withWatermark("event_time", "1 minute")

fixed_window_summary = watermarked_df.filter(col("status") == "paid") \
    .groupBy(window(col("event_time"), "2 minutes"), col("category")) \
    .agg(count("*").alias("fixed_events_count"))

console_query = summary_query.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .start()

file_query = fixed_window_summary.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/output_stream") \
    .option("checkpointLocation", "checkpoints/lab11_fixed") \
    .start()

spark.streams.awaitAnyTermination()