from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("MovingStats") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu từ Kafka
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("price", DoubleType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "btc-price") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.timestamp", "data.price")

moving_stats = df.groupBy(
    window("timestamp", "30 seconds")
).agg(
    avg("price").alias("avg_price"),
    stddev("price").alias("std_price")
)

query = moving_stats.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "btc-price-moving") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()