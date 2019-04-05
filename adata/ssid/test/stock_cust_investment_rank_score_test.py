# coding=utf-8
from testbase import testBase
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, Row
from leek.adata.ssid.stock_cust_investment_rank_score import StockCustInvestRankScore
from sklearn import metrics
from pyspark.sql import functions as F


class SCDHTest(testBase):

    def setUp(self):
        StockCustInvestRankScore.logLevel = 'debug'
        self.scdh = StockCustInvestRankScore(None)
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))

        self.enddate = "2018-03-18"

    def test_get_bspaase_data(self):
        self.scdh._get_base_data("2017-03-16", "2017-03-16", 90)

    def test_get_rm_data(self):
        self.scdh._get_rm_data("2017-03-16", "2017-03-16")

    def test_case(self):
        sql = """
        select max(return_rate_rank),min(return_rate_rank),
        max(alpha_rank),max(cl_value_rank),
        min(cl_value_rank),max(cl_value_score),
        min(cl_value_score),max(total_score),
        min(total_score)
        from stock_cust_investment_rank_score
        where busi_date='2017-03-16' and compute_term=90
        """

    def test_union(self):
        df = self.spark.createDataFrame(
            [{"b": 2, "c": 4, "a": 3}, {"a": 2, "c": 6, "b": 3}, {"a": 5, "c": 2, "b": 3},
             {"a": 1, "c": 3, "b": 3}])
        df.show()
        df2 = self.spark.createDataFrame([{"b": 2, "c": 4, "a": 3}])
        df2.show()
