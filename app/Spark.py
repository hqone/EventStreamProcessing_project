from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, expr, count
from pyspark.sql.functions import window
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, BooleanType

from app.Resources import Resources

"""
PySpark implementation of read kafka stream, aggregate data and write it back to kafka on other stream.
"""
# Start Spark Session
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read kafka stream
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Resources.KAFKA_URI)  # kafka server
      .option("subscribe", Resources.TOPIC_RAW_DATA)  # topic
      # .option("startingOffsets", "earliest")  # start from beginning
      .load())

# Map JSON Event structure
user_schema = StructType([
    StructField("request_end_time_minute", StringType(), True),
    StructField("action", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("is_error", BooleanType(), True),
])

# - Provided key schema:                                StructType(StructField(window,StructType(StructField(start,TimestampType,true), StructField(end,TimestampType,true)),false), StructField(action,StringType,true))
# 2022-05-24T06:43:53.937321096Z - Provided value schema: StructType(StructField(sum,DoubleType,true), StructField(count,LongType,true), StructField(count,LongType,false), StructField(count,LongType,false))
# 2022-05-24T06:43:53.937325076Z - Existing key schema: StructType(StructField(window,StructType(StructField(start,TimestampType,true), StructField(end,TimestampType,true)),false), StructField(action,StringType,true))
# 2022-05-24T06:43:53.937328020Z - Existing value schema: StructType(StructField(sum,DoubleType,true), StructField(count,LongType,true), StructField(count,LongType,false))

# Select json event to columns.
df = (
    df.selectExpr("CAST(value as string)", "timestamp")
        .select(from_json(col("value"), user_schema).alias("json_value"), "timestamp")
        .selectExpr("json_value.*", "timestamp")
        .select(
        col("request_end_time_minute"),
        col("action"),
        col("duration_ms"),
        col("is_error"),
        col("timestamp")
    )

)

# Aggregate data with usage of time windows and watermark.
windowedAvg = (
    df.withWatermark("timestamp", "2 minutes")
        .groupBy(window(col("timestamp"), "1 minutes").alias('eventTimeWindow'), col('action'))
        .agg(avg("duration_ms").alias("avg_duration_ms"), count("*").alias("count"), count(col('is_error')).alias("count_is_error"))
        .select(
        col("eventTimeWindow.start").alias("eventTime"),
        col("avg_duration_ms"),
        col('count'),
        col('action'),
        col('count_is_error')
    )
)

# Write to console for debug, this is not necessary.
qk = windowedAvg.selectExpr("CAST(eventTime AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode('update') \
    .format('console') \
    .option('truncate', 'true') \
    .start()

# # Write back to kafka aggregated data.
query = windowedAvg.selectExpr("CAST(eventTime AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", Resources.KAFKA_URI) \
    .option("topic", Resources.TOPIC_COMPUTED_DATA) \
    .option("checkpointLocation", "/kafkaStream") \
    .outputMode("update") \
    .start()

# Make it run.
qk.awaitTermination()
