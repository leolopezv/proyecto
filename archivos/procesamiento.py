import findspark
findspark.init()
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./libs/spark-sql-kafka-0-10_2.11-2.4.5.jar,./libs/kafka-clients-2.4.1.jar pyspark-shell'
#################################################################

from pyspark.sql import SparkSession

## Create a spark session
spark = SparkSession\
    .builder\
    .appName("SparkDemo")\
    .master("local[*]")\
    .getOrCreate()
#####################################################################

import pyspark.sql.functions as F
from pyspark.sql.types import *

## Kafka configs
kafka_input_config = {
    "kafka.bootstrap.servers" : "kafka:9092",
    "subscribe" : "clima-en-guayaquil",
    "startingOffsets" : "latest",
    "failOnDataLoss" : "false"
}

kafka_output_config = {
    "kafka.bootstrap.servers" : "kafka:9092",
    "topic" : "output",
    "checkpointLocation" : "./check.txt"
}

## Input Schema
df_schema = StructType([
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True)
])


## Read Stream
df = spark\
    .readStream\
    .format("kafka")\
    .options(**kafka_input_config)\
    .load()\
    .select(F.from_json(F.col("value").cast("string"),df_schema).alias("json_data"))\
    .select("json_data.*")

# Add a timestamp column for windowing
df_with_timestamp = df.withColumn("timestamp", F.current_timestamp())

# Perform windowed aggregation
windowed_df = df_with_timestamp\
    .withWatermark("timestamp", "10 seconds")\
    .groupBy(F.window("timestamp", "10 seconds"))\
    .agg(
        F.avg("temperature").alias("avg_temperature"),
        F.avg("humidity").alias("avg_humidity")
    )

# Convert the result to JSON
output_df = windowed_df.select(F.to_json(F.struct(*windowed_df.columns)).alias("value"))

write = output_df\
    .writeStream\
    .format("kafka")\
    .options(**kafka_output_config)\
    .start()

write.awaitTermination()
############################################################

for stream in spark.streams.active:
    print(f"Stopping stream: {stream.id}")
    stream.stop()

############################################################

import shutil
shutil.rmtree('./check.txt', ignore_errors=True)