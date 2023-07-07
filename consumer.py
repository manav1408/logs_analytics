from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, window, to_timestamp, unix_timestamp
from pyspark.sql.types import IntegerType, TimestampType
import pyspark.sql.functions as F
import argparse
import os

spark = SparkSession.builder.appName("de_hw2").getOrCreate()

parser = argparse.ArgumentParser(description='Kafka and spark streaming')

# Add arguments
parser.add_argument('topic', type=str, help='topic to be used')
parser.add_argument('server', type=str, help='server')
parser.add_argument('output_file', type=str, help='output file address')


# Parse the command-line arguments
args = parser.parse_args()
# Access the parsed values
output_file = args.output_file
server = args.server
topic = args.topic

##reading data from stream
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", server) \
        .option("subscribe", topic) \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)")

##parsing data using regex
host_pattern = r"^([^\s]+\s)"
ts_pattern = r"^.*\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})]"
method_uri_protocol_pattern = r'^.*"\s*(\w+)\s+([^\s]+)\s+([^\s]+)"'
status_pattern = r'^.*"\s+([^\s]+)'
content_size_pattern = r"^.*\s+(\d+)$"

df_parsed = df.withColumn("host", regexp_extract(df["value"], host_pattern, 1)) \
        .withColumn("timestamp", regexp_extract(df["value"], ts_pattern, 1)) \
        .withColumn("method", regexp_extract(df["value"], method_uri_protocol_pattern, 1)) \
        .withColumn("endpoint", regexp_extract(df["value"], method_uri_protocol_pattern, 2)) \
        .withColumn("protocol", regexp_extract(df["value"], method_uri_protocol_pattern, 3)) \
        .withColumn("status", regexp_extract(df["value"], status_pattern, 1).cast(IntegerType())) \
        .withColumn("content_size", regexp_extract(df["value"], content_size_pattern, 1).cast(IntegerType()))

timestamp_format = "yyyy-MM-dd HH:mm:ss"
df_parsed = df_parsed.withColumn("timestamp",
                                 to_timestamp(unix_timestamp(df_parsed["timestamp"], timestamp_format).cast("timestamp")))

##fill NA's

# df_parsed = df_parsed[df_parsed['status'].isNotNull()]
# df_parsed = df_parsed.na.fill({'content_size': 0})

################################### EDA #################################################

df_parsed = df_parsed.withWatermark("timestamp", "1 day")

##Analysis on content size - count
path = f'hdfs://localhost:9000/{output_file}/content_size_count3/'
df_content_size_count = df_parsed \
              .groupBy(col("content_size"), window(df_parsed.timestamp, "12 hours"))\
              .count().alias("count")

df_content_size = df_content_size_count.select("content_size","window.start","window.end","count")

# df_content_size.show(10, False)

df_content_size \
        .writeStream \
        .format("parquet") \
        .trigger(processingTime='10 seconds') \
        .outputMode("append") \
        .option("path",path) \
        .option("checkpointLocation", path) \
        .start()


##Analysis on status - count
path = f'hdfs://localhost:9000/{output_file}/status_count3/'
df_status_count = df_parsed \
              .groupBy(col("status"), window(df_parsed.timestamp, "12 hours"))\
              .count().alias("count")

df_status_count2 = df_status_count.select("status","window.start","window.end","count")

df_status_count2.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

##Endpoint counts
path = f'hdfs://localhost:9000/{output_file}/endpoints3'
df_15_endpts = df_parsed.groupBy(col("endpoint"), window(df_parsed.timestamp, "12 hours")).count().alias("count")

df_15_endpts2 = df_15_endpts.select("endpoint","window.start","window.end","count")

df_15_endpts2.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

##Host counts
path = f'hdfs://localhost:9000/{output_file}/host3'
df_host = df_parsed.groupBy(col("host"), window(df_parsed.timestamp, "12 hours")).count()

df_host2 = df_host.select("host","window.start","window.end","count")

df_host2.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

## Error endpoints
path = f'hdfs://localhost:9000/{output_file}/error_endpoints3'
df_err_endpt = df_parsed.filter(~col("status").contains(200)).groupBy(col("endpoint"), window(df_parsed.timestamp, "12 hours")).count()

df_err_endpt2 = df_err_endpt.select("endpoint","window.start","window.end","count")

df_err_endpt2.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

#daily host count
path = f'hdfs://localhost:9000/{output_file}/daily_host_cnt3'
host_day_df = df_parsed.select(df_parsed.host, F.dayofmonth(df_parsed.timestamp).alias('day'), df_parsed.timestamp).drop_duplicates()
daily_host_count = host_day_df.groupBy(col('day'), window(host_day_df.timestamp, "12 hours")).count()

daily_host_count2 = daily_host_count.select("day","window.start","window.end","count")

daily_host_count2.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

#404 endpoints
path = f'hdfs://localhost:9000/{output_file}/endpts_404_3'
df_404_endpt = df_parsed.filter(col("status").contains(404)).groupBy(col("endpoint"), window(df_parsed.timestamp, "12 hours")).count()

df_404_endpt2 = df_404_endpt.select("endpoint","window.start","window.end","count")

df_404_endpt2.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

## 404 endpts per day

path = f'hdfs://localhost:9000/{output_file}/endpts_404_day_3'
df_404_endpt_day = df_parsed.filter(col("status").contains(404)).select(F.dayofmonth(df_parsed.timestamp).alias('day'), df_parsed.timestamp)
df_404_endpt_day2 = df_404_endpt_day.groupBy(col('day'), window(df_404_endpt_day.timestamp, "12 hours")).count()

df_404_endpt_day3 = df_404_endpt_day2.select("day","window.start","window.end","count")

df_404_endpt_day3.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

## 404 endpts per hour

path = f'hdfs://localhost:9000/{output_file}/endpts_404_hour_3'
df_404_endpt_hr = df_parsed.filter(col("status").contains(404)).select(F.hour(df_parsed.timestamp).alias('hour'), df_parsed.timestamp)
df_404_endpt_hr2 = df_404_endpt_hr.groupBy(col('hour'), window(df_404_endpt_hr.timestamp, "12 hours")).count()

df_404_endpt_hr3 = df_404_endpt_hr2.select("hour","window.start","window.end","count")

df_404_endpt_hr3.writeStream.format("parquet").trigger(processingTime='10 seconds').outputMode("append") \
        .option("checkpointLocation", path) \
        .start(path)

spark.streams.awaitAnyTermination()

