"""    
    spark-submit \
    --master local[*] \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/analysis.py
"""

from dependencies.spark import start_spark

def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='analysis',
        files=['configs/etl_config.json'])
    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # # execute ETL pipeline
    # data = extract_data(spark)
    # data_transformed = transform_data(data, config['steps_per_floor'])
    # load_data(data_transformed)

    # # log the success and terminate Spark application
    # log.warn('test_etl_job is finished')
    # spark.stop()
    return None

if __name__ == '__main__': 
    main()
