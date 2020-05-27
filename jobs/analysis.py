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
    spark, log, config, sc = start_spark(
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
        referral_path(df, sc, config["stop_date"])
    # monthly tasks
    if config["monthly"]:
        hourly_visit_pattern(df, config["stop_date"])
        popular_os(df, config["stop_date"])
        popular_browser(df, config["stop_date"])
        country_dist(df, config["stop_date"])
        average_visit_duration(df, config["stop_date"])
        popular_page(df, config["stop_date"])
        

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

# popular os
def extract_time_device(x):
    t = datetime.fromtimestamp(x["visitStartTime"], pytz.timezone("US/Pacific"))
    return (t.strftime("%Y%m%d%H%M%S"), [x['device'].browser, x['device'].deviceCategory, x['device'].isMobile, x['device'].operatingSystem])
def popular_os(df, stopDate):
    deviceExtracted = df.rdd.map(extract_time_device)
    visitGroupByOS = deviceExtracted.map(lambda x: (x[0], x[1][3]))
    # group by day
    visitGroupByOSD = visitGroupByOS.map(lambda x:(x[0][:8], x[1]))
    visitGroupByOSD = visitGroupByOSD.map(lambda x: ((x[0],x[1]), 1)) \
                        .reduceByKey(lambda x, y: x + y)
    visitGroupByOSD = visitGroupByOSD.sortByKey(ascending=True)
    visitGroupByOSD = visitGroupByOSD.map(lambda x: (x[0][0], x[0][1], x[1]))
    # save csv
    filename = 'out/popular_os' + stopDate[:-2]
    visitGroupByOSD_df = visitGroupByOSD.toDF(['time', 'os', 'count'])
    visitGroupByOSD_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

# popular browser
def popular_browser(df, stopDate):
    deviceExtracted = df.rdd.map(extract_time_device)
    visitGroupByBrowser = deviceExtracted.map(lambda x: (x[0], x[1][0]))
    # group by day
    visitGroupByBrowserD = visitGroupByBrowser.map(lambda x:(x[0][:8], x[1]))
    visitGroupByBrowserD = visitGroupByBrowserD.map(lambda x: ((x[0],x[1]), 1)) \
                            .reduceByKey(lambda x, y: x + y)
    visitGroupByBrowserD = visitGroupByBrowserD.sortByKey(ascending=True)
    visitGroupByBrowserD = visitGroupByBrowserD.map(lambda x: (x[0][0], x[0][1], x[1]))
    # save csv
    filename = 'out/popular_browser' + stopDate[:-2]
    visitGroupByBrowserD_df = visitGroupByBrowserD.toDF(['time', 'browser', 'count'])
    visitGroupByBrowserD_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

# location
def extract_country(x):
    t = datetime.fromtimestamp(x["visitStartTime"], pytz.timezone("US/Pacific"))
    return (t.strftime("%Y%m%d"), x['geoNetwork'].country)
def country_dist(df, stopDate):
    country = df.rdd.map(extract_country)
    country = country.map(lambda x: ((x[0],x[1]), 1)) \
                    .reduceByKey(lambda x, y: x + y)
    country = country.map(lambda x: (x[0][0], x[0][1], x[1]))
    # save csv
    filename = 'out/country_dist' + stopDate[:-2]
    country_df = country.toDF(['time', 'country', 'count'])
    country_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

# average visit duration
def extract_hit_time(x):
    t = datetime.fromtimestamp(x["visitStartTime"], pytz.timezone("US/Pacific"))
    return (t.strftime("%Y%m%d"), (x['hits'][-1].time, 1))
def average_visit_duration(df, stopDate):
    duration = df.rdd.map(extract_hit_time).filter(lambda x: x[1][0] != 0)
    duration = duration.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                    .map(lambda x: (x[0], x[1][0]/1000/x[1][1]))
    # save csv
    filename = 'out/average_visit_duration' + stopDate[:-2]
    duration_df = duration.toDF(['time', 'duration'])
    duration_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

# popular page
def extract_hits(x):
    t = datetime.fromtimestamp(x["visitStartTime"], pytz.timezone("US/Pacific"))
    return (t.strftime("%Y%m%d%H"), (x['hits']))
def popular_page(df, stopDate):
    hitPage = df.rdd.map(extract_hits)
    hitPage = hitPage.flatMap(lambda x: [(x[0], h.page.pagePath) for h in x[1]])
    # page count grouped by day
    hitPage = hitPage.map(lambda x: ((x[0][:-2], x[1]), 1)) \
                .reduceByKey(lambda x, y: x + y)
    hitPage = hitPage.map(lambda x: (x[0][0], x[0][1], x[1]))
    # save csv
    filename = 'out/popular_page' + stopDate[:-2]
    hitPage_df = hitPage.toDF(['date', 'pagePath', 'count'])
    hitPage_df.coalesce(1).write.format('csv').options(header='true').save(filename)

    return None

# referral path
def extract_referralPath(x):
    t = datetime.fromtimestamp(x["visitStartTime"], pytz.timezone("US/Pacific"))
    if x['trafficSource'].referralPath != None and x['trafficSource'].referralPath != "/":
        s = str(x['trafficSource'].source)+str(x['trafficSource'].referralPath)
    else:
        s = str(x['trafficSource'].source)
    return (t.strftime("%Y%m%d"), x['hits'][0].page.pagePath, s)
def referralPageFilter(x, topSource, topTarget):
    isIn = False;
    for s in topSource.value:
        if x[1] == s[0]:
            isIn = True
            os = x[1]
            break
    if not isIn:
        os = 'other source'
    isIn = False;
    for t in topTarget.value:
        if x[0] == t[0]:
            isIn = True
            ot = x[0]
            break
    if not isIn:
        ot = 'other target'
    return (os, ot, x[2])
def referral_path(df, sc, stopDate):
    referralPage = df.rdd.map(extract_referralPath)
    referralPageDay = referralPage.map(lambda x: ((x[0], x[1], x[2]), 1)) \
                        .reduceByKey(lambda x, y: x + y)
    # select the last day
    selectedreferralPageDay = referralPageDay.filter(lambda x: x[0][0] == stopDate) \
                                .map(lambda x: (x[0][1], x[0][2], x[1])) 
    # list top source and target
    selectedreferralPageDayTarget = selectedreferralPageDay.map(lambda x: (x[0],x[2])) \
                                        .reduceByKey(lambda x, y: x + y)
    selectedreferralPageDayTarget = selectedreferralPageDayTarget.sortBy(lambda x: x[1],ascending=False)
    selectedreferralPageDayTarget = selectedreferralPageDayTarget.take(5)
    topTarget = sc.broadcast(selectedreferralPageDayTarget)

    selectedreferralPageDaySource = selectedreferralPageDay.map(lambda x: (x[1],x[2])) \
                                        .reduceByKey(lambda x, y: x + y)
    selectedreferralPageDaySource = selectedreferralPageDaySource.sortBy(lambda x: x[1],ascending=False)
    selectedreferralPageDaySource = selectedreferralPageDaySource.take(5)
    topSource = sc.broadcast(selectedreferralPageDaySource)
    # filter top source and target
    referralPageDayFiltered = selectedreferralPageDay.map(lambda x:referralPageFilter(x, topSource, topTarget)) \
                            .map(lambda x: ((x[0],x[1]), x[2])) \
                            .reduceByKey(lambda x, y: x + y)
    referralout = referralPageDayFiltered.collect()
    # modify the schema of json
    referralLinks = []
    referralNodes = []
    for (x, y) in referralout:
        referralLinks.append({
            'source': x[0],
            'target': x[1],
            'value': y
        })
    for n in selectedreferralPageDaySource:
        referralNodes.append({
            "name": n[0]
        })
    for n in selectedreferralPageDayTarget:
        referralNodes.append({
            "name": n[0]
        })
    referralNodes.append({"name": 'other source'})
    referralNodes.append({"name": 'other target'})
    referralDict = {"nodes": referralNodes, "links": referralLinks}
    # save json
    import json
    filepath = 'out/referral_path' + stopDate + ".json"
    with open(filepath, "w") as fout:
        json.dump(referralDict, fout)

    return None


if __name__ == '__main__': 
    main()
