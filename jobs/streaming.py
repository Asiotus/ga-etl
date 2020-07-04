"""    
    spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,mysql:mysql-connector-java:8.0.20 \
    --master local[*] --py-files packages.zip \
    --files configs/etl_config.json jobs/streaming.py

"""



from dependencies.spark import start_spark
import time
from datetime import datetime
import pytz
from pyspark import SparkContext
from pyspark.sql import SQLContext
# from pyspark.sql.functions import explode
# from pyspark.sql.functions import split
# from pyspark.sql.functions import json_tuple
# from pyspark.sql.functions import udf
# from pyspark.sql.functions import window
# from pyspark.sql.functions import col
# from pyspark.sql.functions import to_json
from pyspark.sql.functions import *
from pyspark.sql.types import *


import json
from pyspark.streaming import StreamingContext




def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config, sc = start_spark(
        app_name='streaming',
        files=['configs/etl_config.json'])
    log.warn('***analysis is up-and-running***')
    sc.setLogLevel('WARN')

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "numtest") \
        .load()
    
    print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

    # decode to utf-8
    @udf
    def to_utf(x):
        print("**************to-utf*************")
        return x.decode("utf-8")

    decode_val = df.select(
        to_utf(df.value).alias("json")
    )

    # parse json and convert to DF
    converted_schema = StructType([
        StructField("time", IntegerType(), False),
        StructField("visitorId", StringType(), False)
    ])

    def convert(x):
        print("************convert***************")
        jsondict = json.loads(x)
        return (jsondict["visitStartTime"], jsondict['fullVisitorId'])
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
    convert_df = udf(convert, converted_schema)

    converted = decode_val.select(
        convert_df(decode_val.json).alias("time_visit") 
    )

    converted = converted.select(
        col("time_visit.time").cast(TimestampType()).alias("timestamp"), "time_visit.visitorId"
        )

    # windowedCounts = converted.withWatermark("timestamp", "10 minutes") \
    #     .groupBy(
    #         window(converted.timestamp, "60 minutes", "10 minutes"),
    #         converted.visitorId
    #     ).count()
    
    windowedCounts = converted.withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(converted.timestamp, "1 minutes", "1 minutes"),
        ).count()
    windowedCounts = windowedCounts.withColumnRenamed("count","num")
    windowedCounts = windowedCounts.select(col("window.start").alias("window"), col("num"))
    windowedCounts = windowedCounts.select(to_json(struct("window", "num")).alias("value"))

    db_target_properties = {"user":"root", "password":"12345678","driver":"com.mysql.jdbc.Driver"}

    def writer(x,epoch_id):
        x = x.select(col("window.start").alias("window"), col("num"))
        print("^^^^^^^^^^^^^^^^^^^^^")
        x.write.jdbc(
            url="jdbc:mysql://localhost/ga_streaming", 
            table="visitpermin", 
            mode="append", 
            properties=db_target_properties)
    # def writer(x,epoch_id):
    #     print(x)

    query = windowedCounts \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:2181") \
        .option("checkpointLocation", "~/Documents/google_analysis/kafka_check") \
        .option("topic", "transformed") \
        .outputMode("complete") \
        .start()

    # query = windowedCounts \
    #     .writeStream \
    #     .foreachBatch(writer) \
    #     .outputMode("update") \
    #     .start()

    # query = windowedCounts \
    #     .writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()
    query.awaitTermination()
    log.warn('*** ***')

    return None


# def visit_per_min(df, stopDate):  
#     # group by min 
#     visitGroupByMin = df.rdd.map(extract_visitId_time_min)
#     # select the last day
#     visitGroupByMin = visitGroupByMin.filter(lambda x: x[1][:-4] == stopDate)
#     # count visits
#     visitMin = visitGroupByMin.map(lambda x: (x[1], 1))
#     visitMin = visitMin.reduceByKey(lambda x, y: x + y)
#     visitMin = visitMin.sortByKey(ascending=True)
    
#     return visitHour

if __name__ == '__main__': 
    main()
