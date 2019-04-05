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
        stock_oprd_check_exception_data.trade_id	41025
        stock_oprd_check_exception_data.secu_acc_id
        stock_oprd_check_exception_data.prd_no	2.002722
        stock_oprd_check_exception_data.o_return	-5684.649999999994
        stock_oprd_check_exception_data.o_exception_label	0
        stock_oprd_check_exception_data.prd_return	19203.919999999925
        stock_oprd_check_exception_data.prd_exception_label	0
        stock_oprd_check_exception_data.lr_equal_exception	1
        stock_oprd_check_exception_data.lmv_euqal_exception	0
        stock_oprd_check_exception_data.sr_equal_exception	0
        stock_oprd_check_exception_data.smv_equal_exception	0
        stock_oprd_check_exception_data.busi_date	2017-03-21
        """

        sql = """
            select *
            from fdata.stock_unclose_o_trade_long_data
            where busi_date='2017-03-21' and trade_id='41025' and prd_no='2.002722'
        """
        self.spark.sql(sql)

        sql = """
         select *
            from fdata.stock_unclose_prd_long_data
            where busi_date='2017-03-21' and trade_id='41025' and prd_no='2.002722'
        """
        self.spark.sql(sql)

        sql = """
            select *
            from fdata.stock_cash_flow
            where busi_date='2017-03-21'
               and trade_id='83995' and prd_no='2.000510'
        """
        self.spark.sql(sql)
