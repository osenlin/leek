# coding=utf-8
from testbase import testBase
from leek.adata.ssid.stock_tradebyday_data import StockTradeByDayData
from pyspark.sql import functions as Fn


class SCDHTest(testBase):

    def setUp(self):
        StockTradeByDayData.logLevel = 'debug'
        self.scdh = StockTradeByDayData(None)

    def test_get_base_data(self):
        self.scdh._get_base_info("2017-03-16", "2017-03-16")

    def test__get_cal_current_month_data(self):
        self.scdh._cal_current_month_data("2017-03-16", "2017-03-20")

    def test_cal_current_data(self):
        df = self.scdh.sparkSession.sql("")
        self.scdh._cal_current_data("2017-03-15", "2017-03-20", df)

    def test_casewhen(self):
        df = self.spark.createDataFrame(
            [{"b": 2, "c": 4, "a": 3}, {"a": 2, "c": 6, "b": 3}, {"a": 5, "c": 2, "b": 3},
             {"a": 1, "c": 3, "b": 3}])
        df.createOrReplaceTempView("temp")
        df2 = self.spark.sql("select * from temp")
        df2.show()
        df2 = df2.withColumn("d", Fn.lit(5)).withColumn("c", Fn.lit(5))
        df2.createOrReplaceTempView("temp")
        df3 = self.spark.sql("select * from temp")
        df3.show()

    def all_data_sql(self):
        sql = """
        select max(return_rate),max(weighted_term),max(return),
         min(return_rate),min(weighted_term) ,min(return)
        from adatatest.stock_tradebyday_data
        """
