""" USAGE:
    spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,mysql:mysql-connector-java:8.0.20 \
    --master local[*] --py-files packages.zip \
    --files configs/etl_config.json jobs/streaming.py

"""

import json
import time
from datetime import datetime

import pytz
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext

from dependencies.spark import start_spark


def main():
    # start Spark application and get Spark session, logger and config
    spark, log, config, sc = start_spark(
        app_name="streaming",
        files=["configs/etl_config.json"])
    log.warn("***analysis is up-and-running***")
    sc.setLogLevel("WARN")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "topic1") \
        .load()
    
    print("read stream done")

    # decode to utf-8
    @udf
    def to_utf(x):
        return x.decode("utf-8")

    decode_val = df.select(
        to_utf(df.value).alias("json")
    )

    # parse json and convert to DF
    # extract time, visitorId

    def extract_time_visitorid(x):
        jsondict = json.loads(x)
        return (jsondict["visitStartTime"], jsondict["fullVisitorId"])

    convert_df = udf(extract_time_visitorid, StructType([
        StructField("time", IntegerType(), False),
        StructField("visitorId", StringType(), False)
    ]))

    time_visitid_df = decode_val.select(
        convert_df(decode_val.json).alias("time_visit") 
    )

    time_visitid_df = time_visitid_df.select(
        col("time_visit.time").cast(TimestampType()).alias("timestamp"), "time_visit.visitorId"
        )

    visit_per_min_df = time_visitid_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(window(time_visitid_df.timestamp, "1 minutes", "1 minutes")) \
        .count() \
        .withColumnRenamed("count","num")
    visit_per_min_df = visit_per_min_df.select(col("window.start").alias("begin"), col("num"))
    visit_per_min_df = visit_per_min_df.select(to_json(struct("begin", "num")).alias("value"))

    print("spark stream is running")
    query = visit_per_min_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("checkpointLocation", "/tmp/kafka_check") \
        .option("topic", "transformed2") \
        .outputMode("update") \
        .start()

    query.awaitTermination()


if __name__ == "__main__": 
    main()
