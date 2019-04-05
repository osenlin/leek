# coding=utf-8
from testbase import testBase
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, Row
from leek.adata.ssid.stock_cust_pl_analysis import StockCustPlAnalysis
from pyspark.sql import functions as Fn


class SCDHTest(testBase):

    def setUp(self):
        StockCustPlAnalysis.logLevel = 'debug'
        self.scdh = StockCustPlAnalysis(None)
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))

    def test_get_base_data(self):
        self.scdh._get_base_info("2017-03-16", "2017-03-16")

    def test__get_unclose_data(self):
        self.scdh._get_unclose_data("2017-03-16", "2017-03-16")

    def test_casewhen(self):
        df = self.spark.createDataFrame(
            [{"b": 2, "c": 4, "a": 3}, {"a": 2, "c": 6, "b": 3}, {"a": 5, "c": 2, "b": 3},
             {"a": 1, "c": 3, "b": 3}])
        df.withColumn("hlo", Fn.lit(10)).show()

    def test_sql(self):
        sql = """
            select max(total_gain) max_total_gain,
            max(total_loss) max_total_loss,
            max(total_return) max_total_return,
            max(avg_return_rate) max_avg_return_rate,
            max(avg_gain_rate) max_avg_gain_rate,
            max(avg_loss_rate) max_avg_loss_rate,
            max(trade_num) max_trade_num,
            max(trade_gain_num) max_trade_gain_num,
            max(trade_loss_num) max_trade_loss_num,
            min(total_loss) min_total_loss,
            min(total_return) min_total_return,
            min(avg_return_rate) min_avg_return_rate,
            min(avg_gain_rate) min_avg_gain_rate,
            min(avg_loss_rate) min_avg_loss_rate,
            min(trade_num) min_trade_num,
            min(trade_gain_num) min_trade_gain_num,
            min(trade_loss_num) min_trade_loss_num
            from adatatest.stock_cust_pl_analysis
            where busi_date='2017-03-17'
        """
