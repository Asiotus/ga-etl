"""    
    spark-submit \
    --master local[*] \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/analysis.py
"""

from dependencies.spark import start_spark
import time
from datetime import datetime
import pytz
from pyspark import SparkContext
from pyspark.sql import SQLContext


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='analysis',
        files=['configs/etl_config.json'])
    log.warn('***analysis is up-and-running***')
    # load data
    df = load(spark, config["start_date"], config["stop_date"], config["folder"])
    log.warn('***data loaded***')
    # daily tasks
    if config["daily"]:
        visit_per_hour(df, config["stop_date"])
        visitor_per_hour(df, config["stop_date"])
    # monthly tasks
    if config["monthly"]:
        hourly_visit_pattern(df, config["stop_date"])

    return None

def load(spark, startDate, stopDate, folder):
    import os
    from os import listdir
    from os.path import isfile, join
    
    directory = os.getcwd()+ "/" + folder
    fileList = listdir(directory)
    fileList = [f for f in fileList if 'json' in f]

    selectedDate = [f for f in fileList if (f >= 'ga_sessions_'+startDate+'.json' and f <= 'ga_sessions_'+stopDate+'.json')]
    selectedDatepaths = [directory + f for f in selectedDate]

    df = spark.read.format('json') \
    .option("inferSchema", True) \
    .option("maxColumns", "540000") \
    .option("header", True) \
    .option("sep", "\t") \
    .load(selectedDatepaths)

    return df

# visit per hour
def extract_visitId_time_hour(x):
        t = datetime.fromtimestamp(x["visitStartTime"], pytz.timezone("US/Pacific"))
        return (x['fullVisitorId'], t.strftime("%Y%m%d%H"))
def visit_per_hour(df, stopDate):  
    # group by hour 
    visitGroupByHour = df.rdd.map(extract_visitId_time_hour)
    # select the last day
    visitGroupByHour = visitGroupByHour.filter(lambda x: x[1][:-2] == stopDate)
    # count visits
    visitHour = visitGroupByHour.map(lambda x: (x[1], 1))
    visitHour = visitHour.reduceByKey(lambda x, y: x + y)
    visitHour = visitHour.sortByKey(ascending=True)
    # save csv
    filename = 'out/visit_per_hour' + stopDate
    visitHour_df = visitHour.toDF(['time', 'visits'])
    visitHour_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

# visitors per hour
def visitor_per_hour(df, stopDate):
    # group by hour
    visitGroupByHour = df.rdd.map(extract_visitId_time_hour)
    # select the last day
    visitGroupByHour = visitGroupByHour.filter(lambda x: x[1][:-2] == stopDate)
    # count visitors
    visitorHour = visitGroupByHour.distinct().map(lambda x: (x[1], 1))
    visitorHour = visitorHour.reduceByKey(lambda x, y: x + y)
    visitorHour = visitorHour.sortByKey(ascending=True)
    # save csv
    filename = 'out/visitor_per_hour' + stopDate
    visitorHour_df = visitorHour.toDF(['time', 'visitors'])
    visitorHour_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

# hourly visit pattern 
def hourly_visit_pattern(df, stopDate):
    # group by hour
    visitGroupByHour = df.rdd.map(extract_visitId_time_hour)
    # count by hour
    visitPatternHour = visitGroupByHour.map(lambda x: (x[1][-2:], 1))
    visitPatternHour = visitPatternHour.reduceByKey(lambda x, y: x + y)
    visitPatternHour = visitPatternHour.sortByKey(ascending=True)
    # save csv
    filename = 'out/hourly_visit_pattern' + stopDate[:-2]
    visitPatternHour_df = visitPatternHour.toDF(['time', 'visits'])
    visitPatternHour_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

if __name__ == '__main__': 
    main()
