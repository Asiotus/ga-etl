"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
import pandas as pd
import json
from os import listdir


from pyspark.sql.functions import mean

from dependencies.spark import start_spark
from jobs.analysis import *
from pandas.testing import assert_frame_equal

def read_pd_csv(path, folder, func, stop_date):
    # path = self.test_data_path
    # func = visit_per_hour
    # folder = exp
    # stop_date = self.config["stop_date"]
    filepath = path + folder + '/' + func 
    fileList = listdir(filepath + stop_date)
    filename = [f for f in fileList if f.endswith('csv')]
    return pd.read_csv(filepath + stop_date + "/" + filename[0])

def assert_frame_equal_with_sort(results, expected, keycolumns):
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
    assert_frame_equal(results_sorted, expected_sorted)
class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{
                                "start_date": "20160801",
                                "stop_date": "20160804",
                                "daily": true,
                                "monthly": true,
                                "folder": "tests/test_data/"
                                }""")
        self.spark, _, _, sc = start_spark()
        self.test_data_path = self.config["folder"]
        self.input_data = load(self.spark, self.config["start_date"], self.config["stop_date"], self.test_data_path + "ga/")

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()
    
    def test_visit_per_hour(self):
        expected_visit_per_hour = read_pd_csv(self.test_data_path, "exp", "visit_per_hour", self.config["stop_date"])
        out_visit_per_hour = visit_per_hour(self.input_data, self.config["stop_date"])
        save(out_visit_per_hour, self.test_data_path + 'out/visit_per_hour', self.config["stop_date"])
        out_visit_per_hour = read_pd_csv(self.test_data_path, "out", "visit_per_hour", self.config["stop_date"])
        assert_frame_equal_with_sort(out_visit_per_hour, expected_visit_per_hour, 'time')
    
    def test_visitor_per_hour(self):
        expected_visitor_per_hour = read_pd_csv(self.test_data_path, "exp", "visitor_per_hour", self.config["stop_date"])
        out_visitor_per_hour = visitor_per_hour(self.input_data, self.config["stop_date"])
        save(out_visitor_per_hour, self.test_data_path + 'out/visitor_per_hour', self.config["stop_date"])
        out_visitor_per_hour = read_pd_csv(self.test_data_path, "out", "visitor_per_hour", self.config["stop_date"])
        assert_frame_equal_with_sort(out_visitor_per_hour, expected_visitor_per_hour, 'time')

    def test_hourly_visit_pattern(self):
        expected_hourly_visit_pattern = read_pd_csv(self.test_data_path, "exp", "hourly_visit_pattern", self.config["stop_date"])
        out_hourly_visit_pattern = hourly_visit_pattern(self.input_data, self.config["stop_date"])
        save(out_hourly_visit_pattern, self.test_data_path + 'out/hourly_visit_pattern', self.config["stop_date"])
        out_hourly_visit_pattern = read_pd_csv(self.test_data_path, "out", "hourly_visit_pattern", self.config["stop_date"])
        assert_frame_equal_with_sort(out_hourly_visit_pattern, expected_hourly_visit_pattern, 'time')

    def test_popular_os(self):
        expected_popular_os = read_pd_csv(self.test_data_path, "exp", "popular_os", self.config["stop_date"])
        out_popular_os = popular_os(self.input_data, self.config["stop_date"])
        save(out_popular_os, self.test_data_path + 'out/popular_os', self.config["stop_date"])
        out_popular_os = read_pd_csv(self.test_data_path, "out", "popular_os", self.config["stop_date"])
        assert_frame_equal_with_sort(out_popular_os, expected_popular_os, 'time')

    def test_popular_browser(self):
        expected_popular_browser = read_pd_csv(self.test_data_path, "exp", "popular_browser", self.config["stop_date"])
        out_popular_browser = popular_browser(self.input_data, self.config["stop_date"])
        save(out_popular_browser, self.test_data_path + 'out/popular_browser', self.config["stop_date"])
        out_popular_browser = read_pd_csv(self.test_data_path, "out", "popular_browser", self.config["stop_date"])
        assert_frame_equal_with_sort(out_popular_browser, expected_popular_browser, 'time')

    def test_country_dist(self):
        expected_country_dist = read_pd_csv(self.test_data_path, "exp", "country_dist", self.config["stop_date"])
        out_country_dist = country_dist(self.input_data, self.config["stop_date"])
        save(out_country_dist, self.test_data_path + 'out/country_dist', self.config["stop_date"])
        out_country_dist = read_pd_csv(self.test_data_path, "out", "country_dist", self.config["stop_date"])
        assert_frame_equal_with_sort(out_country_dist, expected_country_dist, 'time')

    def test_average_visit_duration(self):
        expected_average_visit_duration = read_pd_csv(self.test_data_path, "exp", "average_visit_duration", self.config["stop_date"])
        out_average_visit_duration = average_visit_duration(self.input_data, self.config["stop_date"])
        save(out_average_visit_duration, self.test_data_path + 'out/average_visit_duration', self.config["stop_date"])
        out_average_visit_duration = read_pd_csv(self.test_data_path, "out", "average_visit_duration", self.config["stop_date"])
        assert_frame_equal_with_sort(out_average_visit_duration, expected_average_visit_duration, 'time')

    def test_popular_page(self):
        expected_popular_page = read_pd_csv(self.test_data_path, "exp", "popular_page", self.config["stop_date"])
        out_popular_page = popular_page(self.input_data, self.config["stop_date"])
        save(out_popular_page, self.test_data_path + 'out/popular_page', self.config["stop_date"])
        out_popular_page = read_pd_csv(self.test_data_path, "out", "popular_page", self.config["stop_date"])
        assert_frame_equal_with_sort(out_popular_page, expected_popular_page, 'date')


if __name__ == '__main__':
    unittest.main()
