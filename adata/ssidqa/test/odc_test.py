# coding=utf-8
from pyspark import SparkConf, SparkContext

from testbase import testBase
from leek.adata.ssid.stock_tradebyday_data import StockTradeByDayData
from pyspark.sql import functions as Fn, SparkSession


class SCDHTest(testBase):

    def setUp(self):
        testBase.logLevel = 'debug'
        conf = SparkConf()
        self.spark = self.setupSpark(conf, 'locl', 'hl')

    def test_lr_exception(self):
        sql = """
        select * from adata.stock_oprd_check_exception_data
        where prd_return!=o_return
        """
        """
        stock_odc_check_exception_data.trade_id	10279
        stock_odc_check_exception_data.secu_acc_id
        stock_odc_check_exception_data.prd_no	1.600017
        stock_odc_check_exception_data.o_return	0
        stock_odc_check_exception_data.o_exception_label	0
        stock_odc_check_exception_data.dc_return	-90.23999999999978
        stock_odc_check_exception_data.dc_exception_label	0
        stock_odc_check_exception_data.lr_equal_exception	0
        stock_odc_check_exception_data.lmv_euqal_exception	0
        stock_odc_check_exception_data.sr_equal_exception	1
        stock_odc_check_exception_data.smv_equal_exception	0
        stock_odc_check_exception_data.busi_date	2017-03-16
        """

        sql = """
            select *
            from fdata.stock_unclose_o_trade_short_data
            where busi_date='2017-03-16' and trade_id='10279' and prd_no='1.600017'
        """
        self.spark.sql(sql)

        sql = """
            select *
            from fdata.stock_daily_check_data
            where busi_date='2017-03-16' and trade_id='10279' and prd_no='1.600017'
        """
        self.spark.sql(sql)
